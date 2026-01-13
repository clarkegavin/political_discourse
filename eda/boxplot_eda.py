# eda/boxplot_eda.py
from .base import EDAComponent
from logs.logger import get_logger
from visualisations.factory import VisualisationFactory
import os
import pandas as pd
import math
import matplotlib.pyplot as plt
import numpy as np

def _norms(x):
    s = '' if x is None else str(x)
    s_lower = s.strip().lower()
    s_alnum = ''.join(ch.lower() for ch in s if ch.isalnum())
    s_stripped = s.replace('_', '').replace(' ', '').lower()
    return {s, s_lower, s_alnum, s_stripped}

class BoxPlotEDA(EDAComponent):
    """
    Generate a grid of boxplots for numeric columns.

    Params (via kwargs):
    - columns: optional list of columns to include (if omitted, all numeric columns are used)
    - exclude_columns: optional list to exclude
    - ncols: number of subplot columns (layout control)
    - filename: output filename (default: boxplots.png)
    - log_transform_columns: optional list; transforms applied before plotting (same behaviour as correlation EDA)
    """

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("Initialized BoxPlotEDA")

    def run(self, data, target=None, text_field=None, save_path=None, **kwargs):
        if save_path is None:
            save_path = os.getcwd()
        if data is None or not isinstance(data, pd.DataFrame):
            raise ValueError("BoxPlotEDA requires a pandas DataFrame")

        # include list or all numeric columns
        req_cols = kwargs.get('columns') or []
        if isinstance(req_cols, str):
            req_cols = [req_cols]
        try:
            req_cols = list(req_cols)
        except Exception:
            req_cols = []

        exclude = kwargs.get('exclude_columns') or []
        if isinstance(exclude, str):
            exclude = [exclude]
        try:
            exclude = list(exclude)
        except Exception:
            exclude = []

        # build candidate columns
        if req_cols:
            candidates = [c for c in req_cols if c in data.columns]
        else:
            candidates = list(data.select_dtypes(include=['number']).columns)

        # filter out excludes (robust matching)
        exclude_variants = set()
        for ex in exclude:
            try:
                exclude_variants.update(_norms(ex))
            except Exception:
                pass

        cols_to_plot = []
        for c in candidates:
            if exclude_variants.intersection(_norms(c)):
                self.logger.info(f"Excluding column from boxplots: {c}")
                continue
            # ensure numeric
            if not pd.api.types.is_numeric_dtype(data[c]):
                self.logger.info(f"Skipping non-numeric column for boxplot: {c}")
                continue
            cols_to_plot.append(c)

        if not cols_to_plot:
            self.logger.error("No numeric columns to plot for boxplots")
            raise ValueError("No numeric columns to plot for boxplots")

        # optional log transforms
        log_transform = kwargs.get('log_transform_columns') or []
        if isinstance(log_transform, str):
            log_transform = [log_transform]
        try:
            log_transform = list(log_transform)
        except Exception:
            log_transform = []

        df_sub = data[cols_to_plot].copy()
        if log_transform:
            col_variants = {c: _norms(c) for c in df_sub.columns}
            for req in log_transform:
                matched = [c for c, v in col_variants.items() if v.intersection(_norms(req))]
                for col in matched:
                    try:
                        ser = df_sub[col]
                        if not pd.api.types.is_numeric_dtype(ser):
                            ser_num = pd.to_numeric(ser, errors='coerce')
                        else:
                            ser_num = ser.astype(float)
                        if ser_num.dropna().empty:
                            self.logger.warning(f"Column '{col}' has no numeric values for log transform; skipping")
                            continue
                        minv = ser_num.min()
                        if minv <= 0:
                            shift = 1.0 - float(minv)
                            ser_trans = np.log1p(ser_num + shift)
                        else:
                            ser_trans = np.log1p(ser_num)
                        df_sub.loc[:, col] = ser_trans
                        self.logger.info(f"Applied log1p transform to boxplot column: {col}")
                    except Exception as e:
                        self.logger.warning(f"Failed to apply log transform to boxplot column '{col}': {e}")

        # layout
        ncols = int(kwargs.get('ncols', 3))
        total = len(cols_to_plot)
        nrows = int(math.ceil(total / ncols))

        fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(4 * ncols, 3.5 * nrows))
        if total == 1:
            axes = [axes]
        else:
            axes = axes.flatten()

        box_viz = VisualisationFactory.get_visualisation('boxplot', title=None)
        for i, col in enumerate(cols_to_plot):
            ax = axes[i]
            try:
                box_viz.plot(data=df_sub[col].dropna(), ax=ax, title=col)
            except Exception as e:
                self.logger.warning(f"Failed to create boxplot for '{col}': {e}")
                ax.text(0.5, 0.5, f"Error {col}", ha='center')

        # hide unused axes
        for j in range(total, len(axes)):
            try:
                axes[j].set_visible(False)
            except Exception:
                pass

        fig.tight_layout()
        filename = kwargs.get('filename') or 'boxplots.png'
        outpath = os.path.join(save_path, filename)
        fig.savefig(outpath, bbox_inches='tight')
        self.logger.info(f"Saved boxplots to {outpath}")
        return outpath
