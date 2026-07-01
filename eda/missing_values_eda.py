# eda/missing_values_eda.py
from .base import EDAComponent
from logs.logger import get_logger
from visualisations.factory import VisualisationFactory
import os
import pandas as pd
import matplotlib.pyplot as plt
import math


class MissingValuesEDA(EDAComponent):
    """
    Compute answer-rate (percent answered) per entity for one or more grouping columns
    and render boxplots of the distribution of those answer rates.

    Parameters accepted via kwargs:
    - columns: list of grouping columns (required)
    - missing_values_column: column to check for missingness (string or single-element list). If not
      provided, the pipeline's `text_field` will be used.
    - consolidate: bool (default True). If True, create a single figure with one subplot per column.
      If False, create one figure per column and append the column name to the filename.
    - viz_params: visualisation config (dict or list), must specify name: box_plot (or use default)
    - filename: base filename for output when consolidate=True (or used as base when consolidate=False)
    """

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("Initialized MissingValuesEDA")

    def compute_missingness_rate(self, df: pd.DataFrame, group_col: str, missing_col: str) -> pd.DataFrame:
        """
        Returns a DataFrame indexed by group entity with columns: total, answered, answer_rate
        answer_rate is in percent (0-100)
        """
        if group_col not in df.columns:
            raise KeyError(f"Grouping column '{group_col}' not in DataFrame")
        if missing_col not in df.columns:
            raise KeyError(f"Missingness column '{missing_col}' not in DataFrame")

        # Work on a copy and normalise group values (keep Unknown for NaNs)
        tmp = df.copy()
        tmp[group_col] = tmp[group_col].fillna("Unknown").astype(str)

        # Define answered: not null/empty after stripping
        answered_mask = tmp[missing_col].fillna("").astype(str).str.strip() != ""

        total = tmp.groupby(group_col).size()
        answered = tmp[answered_mask].groupby(tmp[group_col]).size()

        rates = pd.concat([total.rename('total'), answered.rename('answered')], axis=1).fillna(0)
        rates['answer_rate'] = rates['answered'] / rates['total'] * 100.0
        rates = rates.reset_index().rename(columns={group_col: 'entity'})
        return rates[['entity', 'total', 'answered', 'answer_rate']]

    def run(self, data, target=None, text_field=None, save_path=None, viz_params=None, **kwargs):
        if save_path is None:
            save_path = os.getcwd()
        # (do not bind a generic 'filename' here; use viz_filename/base_filename locally to avoid shadowing)
        if data is None or not isinstance(data, pd.DataFrame):
            raise ValueError("MissingValuesEDA requires a pandas DataFrame")

        cols = kwargs.get('columns') or []
        if isinstance(cols, str):
            cols = [cols]
        try:
            cols = list(cols)
        except Exception:
            cols = []

        if not cols:
            self.logger.error("No grouping columns specified for MissingValuesEDA")
            raise ValueError("No grouping columns specified for MissingValuesEDA")

        missing_col = kwargs.get('missing_values_column') or kwargs.get('missing_column') or text_field
        # allow list -> take first
        if isinstance(missing_col, (list, tuple)) and missing_col:
            missing_col = missing_col[0]

        if not missing_col:
            self.logger.error("No missing_values_column provided and no text_field available")
            raise ValueError("No missing_values_column provided and no text_field available")

        # Normalize viz config like other EDA components
        viz_cfg = viz_params or kwargs.get('viz_params') or kwargs.get('viz')
        if viz_cfg is None:
            visualisations = []
        elif isinstance(viz_cfg, list):
            visualisations = viz_cfg
        elif isinstance(viz_cfg, dict):
            # support either a dict representing one viz or a dict with key 'visualisations'
            visualisations = viz_cfg.get('visualisations', [viz_cfg])
        else:
            visualisations = []

        # If no viz config provided, create a sensible default
        if not visualisations:
            viz_filename = kwargs.get('filename') or 'missing_values.png'
            visualisations = [{
                'name': 'boxplot',
                'filename': viz_filename,
            }]

        consolidate = kwargs.get('consolidate', True)

        existing_cols = [c for c in cols if c in data.columns]
        missing = [c for c in cols if c not in data.columns]
        if missing:
            self.logger.warning(f"The following grouping columns were not found and will be ignored: {missing}")
        if not existing_cols:
            self.logger.error("None of the specified grouping columns were found in the DataFrame")
            raise KeyError("None of the specified grouping columns were found in the DataFrame")

        output_files = []
        os.makedirs(save_path, exist_ok=True)

        # Consolidated single figure
        if consolidate:
            total = len(existing_cols)
            # support ncols param for layout; default 1 to remain backwards compatible
            try:
                ncols = int(kwargs.get('ncols', 1))
            except Exception:
                ncols = 1
            if ncols < 1:
                ncols = 1
            nrows = int(math.ceil(total / ncols)) if total > 0 else 1
            fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(4 * ncols, 3.5 * nrows))
            # normalize axes to a flat list for iteration
            if not hasattr(axes, 'flatten'):
                axes = [axes]
            else:
                axes = axes.flatten()

            # Use the first visualisation config to instantiate the boxplot visualiser (we expect name=boxplot)
            cfg = visualisations[0] if visualisations else {}
            # ensure cfg is a mapping/dict; some pipeline configs may pass lists accidentally
            if isinstance(cfg, list) and cfg:
                # prefer a dict inside the list
                first = cfg[0]
                if isinstance(first, dict):
                    cfg = first
                else:
                    cfg = {}
            if not isinstance(cfg, dict):
                cfg = {}
            vis_name = cfg.get('name') or cfg.get('type') or 'boxplot'
            # support common alias 'box_plot' -> 'boxplot'
            if isinstance(vis_name, str) and vis_name.lower() in ('box_plot', 'box-plot'):
                vis_name = 'boxplot'
            # ensure vis_params is a dict before expanding
            try:
                vis_params = dict(cfg)
            except Exception:
                vis_params = {}

            # Build a sanitized kwargs mapping for visualiser init to avoid expanding arbitrary lists/dicts
            safe_keys = {'figsize', 'title', 'ylabel', 'xlabel', 'xticks_rotation'}
            safe_kwargs = {}
            if isinstance(vis_params, dict):
                for k, v in vis_params.items():
                    if k in safe_keys and not isinstance(v, (list, dict)):
                        safe_kwargs[k] = v
            else:
                self.logger.debug(f"vis_params is not a dict, ignoring extras: {type(vis_params)}")

            # ensure we always set ylabel for clarity
            safe_kwargs.setdefault('ylabel', 'Answer Rate (%)')
            # determine output filename (use viz config filename if provided)
            viz_filename = cfg.get('filename') or kwargs.get('filename') or 'missing_values.png'
            box_viz = VisualisationFactory.get_visualisation(vis_name, title=None, **safe_kwargs)

            for i, col in enumerate(existing_cols):
                ax = axes[i]
                try:
                    rates_df = self.compute_missingness_rate(data, col, missing_col)
                    ser = rates_df['answer_rate']
                    # plot into provided axis
                    box_viz.plot(data=ser, ax=ax, title=col)
                except Exception as e:
                    self.logger.warning(f"Failed to create missingness boxplot for '{col}': {e}")
                    ax.text(0.5, 0.5, f"Error {col}", ha='center')

            # hide any unused axes
            for j in range(total, len(axes)):
                try:
                    axes[j].set_visible(False)
                except Exception:
                    pass

            # overall title
            try:
                fig.suptitle('Answer Rate Distribution by Dimension')
            except Exception:
                pass
            fig.tight_layout(rect=(0, 0.03, 1, 0.95))

            outpath = os.path.join(save_path, viz_filename)
            try:
                # Prefer using viz save utility
                if hasattr(box_viz, 'save'):
                    box_viz.save(fig, outpath)
                else:
                    fig.savefig(outpath, bbox_inches='tight')
                output_files.append(outpath)
                plt.close(fig)
                self.logger.info(f"Saved consolidated missingness boxplots to {outpath}")
            except Exception as e:
                self.logger.exception(f"Failed to save consolidated missingness boxplots: {e}")

        else:
            # One figure per column
            cfg = visualisations[0] if visualisations else {}
            # defensive normalization as above
            if isinstance(cfg, list) and cfg:
                first = cfg[0]
                if isinstance(first, dict):
                    cfg = first
                else:
                    cfg = {}
            if not isinstance(cfg, dict):
                cfg = {}
            vis_name = cfg.get('name') or cfg.get('type') or 'boxplot'
            # support common alias 'box_plot' -> 'boxplot'
            if isinstance(vis_name, str) and vis_name.lower() in ('box_plot', 'box-plot'):
                vis_name = 'boxplot'
            try:
                vis_params = dict(cfg)
            except Exception:
                vis_params = {}

            base_filename = cfg.get('filename') or kwargs.get('filename') or 'missing_values.png'
            for col in existing_cols:
                try:
                    rates_df = self.compute_missingness_rate(data, col, missing_col)
                    ser = rates_df['answer_rate']

                    viz_init_kwargs = {}
                    if isinstance(vis_params, dict):
                        for k, v in vis_params.items():
                            if k in ('figsize', 'title', 'ylabel', 'xlabel', 'xticks_rotation') and not isinstance(v, (list, dict)):
                                viz_init_kwargs[k] = v
                    viz_init_kwargs.setdefault('ylabel', 'Answer Rate (%)')
                    box_viz = VisualisationFactory.get_visualisation(vis_name, title=None, **viz_init_kwargs)
                    # Let visualiser create its own figure
                    fig, ax = box_viz.plot(data=ser, title=col)

                    fname = f"{os.path.splitext(base_filename)[0]}_{col}.png"
                    outpath = os.path.join(save_path, fname)
                    if hasattr(box_viz, 'save'):
                        box_viz.save(fig, outpath)
                    else:
                        fig.savefig(outpath, bbox_inches='tight')
                    plt.close(fig)
                    output_files.append(outpath)
                    self.logger.info(f"Saved missingness boxplot for '{col}' to {outpath}")
                except Exception as e:
                    self.logger.exception(f"Failed to create missingness boxplot for '{col}': {e}")

        return {'visualisations': output_files}
