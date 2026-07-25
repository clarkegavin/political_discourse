# eda/tokenize_text_eda.py
from .base import EDAComponent
from logs.logger import get_logger
from visualisations.factory import VisualisationFactory
import os
import pandas as pd
import re
import math
import matplotlib.pyplot as plt


def simple_tokenise(text):
    # lightweight tokenisation (no external dependency)
    return re.findall(r"\b\w+\b", str(text))


class TokenizeTextEDA(EDAComponent):
    """
    Tokenize specified text columns and compute token-length statistics.

    Params (via kwargs):
    - columns: list of column names to tokenize
    - output_column: optional name to store token lists (not persisted here)
    - ncols: layout columns for subplots (defaults: for bar_chart subplots use ncols, for boxplot per-column layout uses ncols)
    - viz_params: visualisation specification(s) (list or dict)
    - filename: base filename if not provided inside viz_params
    """

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("Initialized TokenizeTextEDA")

    def _compute_token_lengths(self, df: pd.DataFrame, col: str) -> pd.Series:
        ser = df[col].fillna("").astype(str)
        return ser.apply(lambda x: len(simple_tokenise(x)))

    def run(self, data, target=None, text_field=None, save_path=None, viz_params=None, **kwargs):
        if save_path is None:
            save_path = os.getcwd()
        if data is None or not isinstance(data, pd.DataFrame):
            raise ValueError("TokenizeTextEDA requires a pandas DataFrame")

        cols = kwargs.get('columns') or []
        if isinstance(cols, str):
            cols = [cols]
        try:
            cols = [c for c in list(cols) if c in data.columns]
        except Exception:
            cols = []

        if not cols:
            self.logger.error("No columns specified or none found in DataFrame for TokenizeTextEDA")
            raise ValueError("No columns specified or none found in DataFrame for TokenizeTextEDA")

        # compute token lengths and basic stats
        token_stats = {}
        token_series = {}
        for col in cols:
            tl = self._compute_token_lengths(data, col)
            token_series[col] = tl
            token_stats[col] = {
                'min_tokens': int(tl.min()) if not tl.dropna().empty else 0,
                'max_tokens': int(tl.max()) if not tl.dropna().empty else 0,
                'avg_tokens': float(tl.mean()) if not tl.dropna().empty else 0.0,
                'median_tokens': float(tl.median()) if not tl.dropna().empty else 0.0,
            }

        # normalize viz_params
        viz_cfg = viz_params or kwargs.get('viz_params') or kwargs.get('viz')
        if viz_cfg is None:
            visualisations = []
        elif isinstance(viz_cfg, list):
            visualisations = viz_cfg
        elif isinstance(viz_cfg, dict):
            visualisations = viz_cfg.get('visualisations', [viz_cfg])
        else:
            visualisations = []

        if not visualisations:
            # default to bar chart of stats
            base_fname = kwargs.get('filename') or 'token_stats.png'
            visualisations = [{'name': 'bar_chart', 'filename': base_fname}]

        os.makedirs(save_path, exist_ok=True)
        output_files = []

        for vc in visualisations:
            if not isinstance(vc, dict):
                self.logger.warning(f"Skipping invalid viz config: {vc}")
                continue
            vis_name = vc.get('name') or vc.get('type') or 'bar_chart'
            # support alias
            if isinstance(vis_name, str) and vis_name.lower() in ('box_plot', 'boxplot', 'box-plot'):
                vis_name = 'boxplot'
            if isinstance(vis_name, str) and vis_name.lower() in ('bar_chart', 'bar-chart'):
                vis_name = 'bar_chart'

            # collect safe init kwargs
            init_kwargs = {
                k: v for k, v in vc.items()
                if k not in ('name', 'filename', 'ncols')
            }

            filename = vc.get('filename') or kwargs.get('filename') or f"token_stats_{vis_name}.png"

            # For bar_chart: create subplots for each statistic (min,max,avg,median)
            if vis_name == 'bar_chart':
                stats = ['min_tokens', 'max_tokens', 'avg_tokens', 'median_tokens']
                total = len(stats)
                try:
                    ncols = int(vc.get('ncols', kwargs.get('ncols', 2)))
                except Exception:
                    ncols = 2
                if ncols < 1:
                    ncols = 1
                nrows = int(math.ceil(total / ncols))
                fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(4 * ncols, 3.5 * nrows))
                if not hasattr(axes, 'flatten'):
                    axes = [axes]
                else:
                    axes = axes.flatten()

                bar_viz = VisualisationFactory.get_visualisation('bar_chart', title=None, ylabel=None, **init_kwargs)

                for i, stat in enumerate(stats):
                    ax = axes[i]
                    # build mapping col->value
                    mapping = {col: token_stats[col][stat] for col in cols}
                    try:
                        bar_viz.plot(data=mapping, ax=ax, title=stat)
                    except Exception as e:
                        self.logger.warning(f"Failed to plot bar chart for stat {stat}: {e}")
                        ax.text(0.5, 0.5, f"Error {stat}", ha='center')

                # hide unused axes
                for j in range(total, len(axes)):
                    try:
                        axes[j].set_visible(False)
                    except Exception:
                        pass

                fig.tight_layout()
                outpath = os.path.join(save_path, filename)
                try:
                    bar_viz.save(fig, outpath)
                    output_files.append(outpath)
                    plt.close(fig)
                    self.logger.info(f"Saved token stats bar chart to {outpath}")
                except Exception as e:
                    self.logger.exception(f"Failed to save token stats bar chart: {e}")

            elif vis_name == 'boxplot':
                # Create boxplots of token length distributions: one subplot per column
                total = len(cols)
                try:
                    ncols = int(vc.get('ncols', kwargs.get('ncols', 1)))
                except Exception:
                    ncols = 1
                if ncols < 1:
                    ncols = 1
                nrows = int(math.ceil(total / ncols)) if total > 0 else 1
                fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(4 * ncols, 3.5 * nrows))
                if not hasattr(axes, 'flatten'):
                    axes = [axes]
                else:
                    axes = axes.flatten()

                #box_viz = VisualisationFactory.get_visualisation('boxplot', title=None, ylabel='Token count')

                box_kwargs = dict(init_kwargs)

                if "ylabel" not in box_kwargs:
                    box_kwargs["ylabel"] = "Token count"

                box_viz = VisualisationFactory.get_visualisation(
                    "boxplot",
                    **box_kwargs
                )

                for i, col in enumerate(cols):
                    ax = axes[i]
                    try:
                        series = token_series[col].dropna()
                        box_viz.plot(data=series, ax=ax, title=col)
                    except Exception as e:
                        self.logger.warning(f"Failed to create boxplot for column {col}: {e}")
                        ax.text(0.5, 0.5, f"Error {col}", ha='center')

                # hide unused axes
                for j in range(total, len(axes)):
                    try:
                        axes[j].set_visible(False)
                    except Exception:
                        pass

                fig.tight_layout()
                outpath = os.path.join(save_path, filename)
                try:
                    box_viz.save(fig, outpath)
                    output_files.append(outpath)
                    plt.close(fig)
                    self.logger.info(f"Saved token distributions boxplot to {outpath}")
                except Exception as e:
                    self.logger.exception(f"Failed to save token distributions boxplot: {e}")

            else:
                self.logger.warning(f"Visualisation '{vis_name}' not supported for TokenizeTextEDA; skipping")

        return {'token_stats': token_stats, 'visualisations': output_files}
