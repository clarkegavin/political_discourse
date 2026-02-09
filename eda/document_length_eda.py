# eda/document_length_eda.py
from .base import EDAComponent
from logs.logger import get_logger
from visualisations.factory import VisualisationFactory
import os
import pandas as pd
import re


class DocumentLengthEDA(EDAComponent):
    """
    EDA step to compute document length across one or more text columns.

    Expected kwargs (via pipeline YAML):
      - columns: list of column names containing document text units (if omitted, will try `text_field`)
      - unit: one of 'characters', 'words', or 'sentences' (default: 'characters')
      - output_column: optional name for the generated column (default: document_length_<unit>)
      - viz_params: dict describing one or more visualisations. Two formats supported:
          * Single visualiser config: { 'name': 'histogram', 'bins': 20, ... }
          * Multiple visualisations: { 'visualisations': [ { 'name': 'histogram', ... }, { 'name': 'boxplot', ... } ] }
      - filename: optional base filename (if provided, will be used as-is for single viz; otherwise each viz gets a generated name)

    Behaviour:
      - Treat missing/null values as empty strings
      - Cast non-string values to str()
      - Character length = len(str(text))
      - Word length = number of whitespace-separated tokens (str.split())
      - Sentence length = number of sentence segments after splitting on punctuation [.!?]+ (empty segments ignored)
      - Adds the computed numeric column to the DataFrame and returns the modified DataFrame
      - Produces visualisations via VisualisationFactory if viz_params provided
      - Logs basic summary statistics (min, max, mean)
    """

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("Initialized DocumentLengthEDA")

    def _compute_unit_length(self, text: str, unit: str) -> int:
        if text is None:
            text = ""
        s = str(text)
        if unit == 'characters':
            return len(s)
        if unit == 'words':
            # split on whitespace
            tokens = s.split()
            return len(tokens)
        if unit == 'sentences':
            # split on sentence-ending punctuation; count non-empty segments
            parts = re.split(r'[.!?]+', s)
            parts = [p for p in parts if p and p.strip()]
            return len(parts)
        # fallback to characters
        return len(s)

    def run(self, data, target=None, text_field=None, save_path=None, **kwargs):
        if save_path is None:
            save_path = os.getcwd()
        if data is None or not isinstance(data, pd.DataFrame):
            raise ValueError("DocumentLengthEDA requires a pandas DataFrame")

        cols = kwargs.get('columns') or []
        if isinstance(cols, str):
            cols = [cols]
        try:
            cols = list(cols)
        except Exception:
            cols = []

        # If no columns specified, try using the pipeline text_field
        if not cols:
            if text_field:
                cols = [text_field]
            else:
                self.logger.error("No columns specified for DocumentLengthEDA and no text_field available")
                raise ValueError("No columns specified for DocumentLengthEDA and no text_field available")

        # filter to existing columns
        cols = [c for c in cols if c in data.columns]
        if not cols:
            self.logger.error("None of the specified columns were found in the DataFrame")
            raise KeyError("None of the specified columns were found in the DataFrame")

        unit = (kwargs.get('unit') or 'characters').lower()
        if unit not in ('characters', 'words', 'sentences'):
            self.logger.warning(f"Unknown unit '{unit}'; defaulting to 'characters'")
            unit = 'characters'

        output_column = kwargs.get('output_column') or f"document_length_{unit}"

        # compute length per row by summing lengths across the selected columns
        def _row_length(row):
            total = 0
            for c in cols:
                try:
                    val = row.get(c, '') if hasattr(row, 'get') else row[c]
                except Exception:
                    try:
                        val = row[c]
                    except Exception:
                        val = ''
                if pd.isna(val):
                    val = ''
                total += self._compute_unit_length(val, unit)
            return total

        # create a copy of data or operate in-place? We'll modify a copy and return it
        df = data.copy()
        df[output_column] = df.apply(_row_length, axis=1).astype(int)

        # log summary statistics
        try:
            ser = df[output_column]
            self.logger.info(f"Document length stats ({output_column}) - min: {int(ser.min())}, max: {int(ser.max())}, mean: {float(ser.mean()):.2f}")
        except Exception as e:
            self.logger.warning(f"Failed to compute summary stats for '{output_column}': {e}")

        # handle visualisations
        # viz_cfg = kwargs.get('viz_params') or {}
        # visualisations = viz_cfg.get('visualisations') if isinstance(viz_cfg, dict) else None
        # if visualisations is None:
        #     # treat the viz_cfg itself as a single visualiser config if it contains a name
        #     if isinstance(viz_cfg, dict) and ('name' in viz_cfg or viz_cfg):
        #         visualisations = [viz_cfg]
        #     else:
        #         visualisations = []
        viz_cfg = kwargs.get('viz_params')

        # Normalize viz configs to a list of dicts
        if viz_cfg is None:
            visualisations = []
        elif isinstance(viz_cfg, list):
            visualisations = viz_cfg
        elif isinstance(viz_cfg, dict):
            # support both single viz or {visualisations: [...]}
            visualisations = viz_cfg.get('visualisations', [viz_cfg])
        else:
            visualisations = []

        self.logger.info(
            f"DocumentLengthEDA will generate {len(visualisations)} visualisations"
        )

        output_files = []
        if visualisations:
            # ensure save path exists
            os.makedirs(save_path, exist_ok=True)

            # standardise to list of dicts
            for idx, vc in enumerate(visualisations):
                if not isinstance(vc, dict):
                    # skip invalid configs
                    self.logger.warning(f"Skipping invalid visualisation config: {vc}")
                    continue
                vis_name = vc.get('name') or vc.get('type') or 'histogram'
                # make a shallow copy of params and remove name
                vis_params = dict(vc)

                # Remove keys that are not valid for the plot method
                for k in ['name', 'type', 'filename', 'output_dir']:
                    vis_params.pop(k, None)

                # vis_params.pop('name', None)
                # vis_params.pop('type', None)
                # vis_params.pop('filename', None)
                # vis_params.pop('output_dir', None)
                # ensure output_dir so certain visualisers can save interactives
                #vis_params.setdefault('output_dir', save_path)

                viz = VisualisationFactory.get_visualisation(vis_name, **vis_params)
                if viz is None:
                    self.logger.warning(f"Visualisation '{vis_name}' not found; skipping")
                    continue

                # determine filename
                filename = vc.get('filename')
                if not filename:
                    filename = f"{output_column}_{vis_name}.png"

                outpath = os.path.join(save_path, filename)

                try:
                    # call plot and save using visualiser
                    #fig_ax = viz.plot(data=df[output_column], title=vc.get('title') or output_column)
                    # call plot
                    fig_ax = viz.plot(
                        data=df[output_column],
                        title=vc.get('title') or output_column,
                        **vis_params
                    )

                    # viz.plot may return (fig, ax) or other tuple-like; pick first element as fig when possible
                    if isinstance(fig_ax, tuple) and len(fig_ax) >= 1:
                        fig = fig_ax[0]
                    else:
                        fig = fig_ax
                    try:
                        viz.save(fig, outpath)
                        output_files.append(outpath)
                        self.logger.info(f"Saved visualisation '{vis_name}' to {outpath}")
                    except Exception as e:
                        self.logger.warning(f"Failed to save visualisation '{vis_name}' to {outpath}: {e}")
                except Exception as e:
                    self.logger.warning(f"Failed to create visualisation '{vis_name}': {e}")

        # return the modified dataframe (and also output file list via dict for compatibility)
        return {
            'dataframe': df,
            'output_column': output_column,
            'visualisations': output_files,
        }

