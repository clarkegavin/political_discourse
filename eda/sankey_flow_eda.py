# eda/sankey_flow_eda.py
from .base import EDAComponent
from logs.logger import get_logger
from visualisations.factory import VisualisationFactory
import os
import pandas as pd
from collections import OrderedDict


class SankeyFlowEDA(EDAComponent):
    """
    Computes transitions between ordered categorical columns and returns Sankey-compatible structure.

    Returns dict:
    {
      "nodes": [label1, label2, ...],
      "links": {"source": [...], "target": [...], "value": [...]}
    }
    """

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("Initialized SankeyFlowEDA")

    def _index_nodes(self, cols, df):
        # deterministic ordering: iterate columns left-to-right and collect unique values in appearance order
        labels = []
        seen = set()
        for c in cols:
            if c not in df.columns:
                continue
            vals = pd.Series(df[c].dropna().astype(str).values)
            for v in vals:
                if v not in seen:
                    seen.add(v)
                    labels.append(v)
        return labels

    def _aggregate_transitions(self, df, cols):
        # For each consecutive pair, compute counts of transitions
        # returns lists: source_labels, target_labels, values
        srcs = []
        tgts = []
        vals = []
        for i in range(len(cols) - 1):
            left = cols[i]
            right = cols[i + 1]
            if left not in df.columns or right not in df.columns:
                self.logger.warning(f"Column pair ({left}, {right}) not present in DataFrame; skipping")
                continue
            pair = df[[left, right]].fillna("Unknown").astype(str)
            # count occurrences
            grouped = pair.groupby([left, right]).size().reset_index(name='count')
            for _, row in grouped.iterrows():
                srcs.append(row[left])
                tgts.append(row[right])
                vals.append(int(row['count']))
        return srcs, tgts, vals

    def run(self, data, target=None, text_field=None, save_path=None, viz_params=None, **kwargs):
        """
        data: pandas DataFrame
        kwargs expects 'columns': ordered list of categorical column names
        viz_params: optional visualisation configs (list/dict)
        """
        if save_path is None:
            save_path = os.getcwd()

        if data is None or not isinstance(data, pd.DataFrame):
            raise ValueError("SankeyFlowEDA requires a pandas DataFrame")

        cols = kwargs.get('columns') or []
        if isinstance(cols, str):
            cols = [cols]
        try:
            cols = list(cols)
        except Exception:
            cols = []

        if not cols:
            self.logger.error("No columns specified for SankeyFlowEDA")
            raise ValueError("No columns specified for SankeyFlowEDA")

        # Ensure we operate only on existing columns (warn about missing ones)
        existing_cols = [c for c in cols if c in data.columns]
        missing = [c for c in cols if c not in data.columns]
        if missing:
            self.logger.warning(f"The following columns were not found and will be ignored: {missing}")

        if not existing_cols:
            self.logger.error("None of the specified columns were found in the DataFrame")
            raise KeyError("None of the specified columns were found in the DataFrame")

        # Work on a copy and normalise NaNs to 'Unknown'
        df = data.copy()
        df[existing_cols] = df[existing_cols].fillna("Unknown").astype(str)

        if len(df) == 0:
            self.logger.warning("Empty DataFrame provided to SankeyFlowEDA")
            return {"nodes": [], "links": {"source": [], "target": [], "value": []}}

        # Build deterministic node list
        nodes = self._index_nodes(existing_cols, df)
        node_index = {label: idx for idx, label in enumerate(nodes)}

        # Aggregate transitions across adjacent column pairs
        src_labels, tgt_labels, values = self._aggregate_transitions(df, existing_cols)

        # Map labels to indices, filter out any unseen labels (shouldn't happen)
        sources = []
        targets = []
        final_values = []
        for s, t, v in zip(src_labels, tgt_labels, values):
            if s not in node_index:
                # add new node deterministically at end
                node_index[s] = len(nodes)
                nodes.append(s)
            if t not in node_index:
                node_index[t] = len(nodes)
                nodes.append(t)
            sources.append(node_index[s])
            targets.append(node_index[t])
            final_values.append(v)

        # Log cardinality warning
        if len(nodes) > 1000:
            self.logger.warning(f"High cardinality in Sankey nodes: {len(nodes)} nodes may be difficult to visualise")

        flow_data = {
            "nodes": nodes,
            "links": {
                "source": sources,
                "target": targets,
                "value": final_values,
            },
            "columns": existing_cols,
        }

        # Handle visualisations if requested
        viz_cfg = viz_params or kwargs.get('viz_params')
        # normalise viz config
        if viz_cfg is None:
            visualisations = []
        elif isinstance(viz_cfg, list):
            visualisations = viz_cfg
        elif isinstance(viz_cfg, dict):
            visualisations = viz_cfg.get('visualisations', [viz_cfg])
        else:
            visualisations = []

        output_files = []
        if visualisations:
            os.makedirs(save_path, exist_ok=True)
            for vc in visualisations:
                if not isinstance(vc, dict):
                    self.logger.warning(f"Skipping invalid visualisation config: {vc}")
                    continue
                vis_name = vc.get('name') or vc.get('type') or 'sankey'
                vis_params = dict(vc)
                # strip non-init keys
                for k in ['name', 'type', 'filename', 'output_dir']:
                    vis_params.pop(k, None)

                viz = VisualisationFactory.get_visualisation(vis_name, **vis_params)
                if viz is None:
                    self.logger.warning(f"Visualisation '{vis_name}' not found; skipping")
                    continue

                filename = vc.get('filename') or f"sankey_{'_'.join(existing_cols)}.html"
                outpath = os.path.join(save_path, filename)
                try:
                    # Plot and save
                    result = viz.plot(data=flow_data, save_path=save_path, filename=filename, title=vc.get('title'))
                    # Some visualisers return path or list
                    if isinstance(result, str) and result:
                        output_files.append(result)
                    elif isinstance(result, (list, tuple)):
                        output_files.extend([r for r in result if isinstance(r, str)])
                    else:
                        output_files.append(outpath)
                    self.logger.info(f"Saved visualisation '{vis_name}' to {outpath}")
                except Exception as e:
                    self.logger.exception(f"Failed to create visualisation '{vis_name}': {e}")

        return {
            'dataframe': df,
            'flow': flow_data,
            'visualisations': output_files,
        }

