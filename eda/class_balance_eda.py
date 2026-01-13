"""
Update ClassBalanceEDA to plot class balance for a single target or for all columns when target is None.
- Numeric columns -> histogram
- Categorical columns -> frequency bar chart
"""
from .base import EDAComponent
from logs.logger import get_logger
from visualisations.factory import VisualisationFactory
import os
import math
import pandas as pd

class ClassBalanceEDA(EDAComponent):
    """
    EDA component to analyze and visualize class balance in the target variable.
    When `target` is None, produce a multi-panel figure showing distributions for
    all columns: histograms for numeric columns and bar charts for categorical columns.
    """

    def __init__(self):
        self.logger = get_logger("ClassBalanceEDA")
        self.logger.info("Initialized ClassBalanceEDA component")

    def _is_categorical(self, series: pd.Series) -> bool:
        # treat object, category, bool or low-cardinality numeric as categorical
        # pandas.is_categorical is deprecated; use dtype check for CategoricalDtype
        try:
            is_cat_dtype = isinstance(series.dtype, pd.CategoricalDtype)
        except Exception:
            is_cat_dtype = False

        if pd.api.types.is_object_dtype(series) or is_cat_dtype or pd.api.types.is_bool_dtype(series):
            return True
        # numeric with small number of unique values -> categorical
        if pd.api.types.is_numeric_dtype(series) and series.nunique(dropna=True) <= 20:
            return True
        return False

    def run(self, data, target=None, text_field=None, save_path=None, **kwargs):
        """
        Analyze and visualize class balance in the target variable or for all columns.

        Parameters:
        - data: pandas DataFrame
        - target: optional column name to show a single class balance plot
        - save_path: directory where to save the output image
        - kwargs: additional parameters forwarded to visualisations

        Returns:
        - filepath: path to the saved image
        """
        self.logger.info(f"Running ClassBalanceEDA on target: {target}")

        if save_path is None:
            save_path = os.getcwd()

        filename = kwargs.get('filename') or 'class_balance.png'
        filepath = os.path.join(save_path, filename)

        #filepath = os.path.join(save_path, "class_balance.png")

        # If a specific target is provided, keep original behaviour
        if target is not None:
            if target not in data.columns:
                self.logger.error(f"Target column '{target}' not found in data")
                raise ValueError(f"Target column '{target}' not found in data")

            # Pass the raw Series (not a pre-computed dict) to the BarChart visualiser.
            # The BarChart will compute value_counts and draw categories on the x-axis
            # and counts on the y-axis.
            self.logger.info(f"Creating class balance plot for target column: {target}")

            viz = VisualisationFactory.get_visualisation(
                "bar_chart",
                title=f"Class Balance: {target}",
                xlabel=target,
                xticks_rotation=45,
                ylabel="Count",
                figsize=(10, 6),
                **kwargs,
            )

            fig, ax = viz.plot(data=data[target])
            viz.save(fig, filepath)
            return filepath

        # When target is None: create subplots for all columns
        if not isinstance(data, pd.DataFrame):
            self.logger.error("`data` must be a pandas DataFrame when target is None")
            raise ValueError("`data` must be a pandas DataFrame when target is None")

        cols = list(data.columns)
        if len(cols) == 0:
            self.logger.error("Empty DataFrame provided")
            raise ValueError("Empty DataFrame provided")

        # Handle exclude_columns passed via kwargs (from pipeline YAML)
        exclude = kwargs.get('exclude_columns') or []
        if isinstance(exclude, str):
            exclude = [exclude]
        try:
            exclude = list(exclude)
        except Exception:
            exclude = []

        # Handle log transform columns and histogram bins from kwargs
        log_transform_cols = kwargs.get('log_transform_columns') or []
        if isinstance(log_transform_cols, str):
            log_transform_cols = [log_transform_cols]
        try:
            log_transform_cols = list(log_transform_cols)
        except Exception:
            log_transform_cols = []

        # hist_bins can be an int (global) or dict mapping column->bins
        hist_bins_cfg = kwargs.get('hist_bins')

        # Normalization helper used to match column names robustly
        def _norms(x):
            s = '' if x is None else str(x)
            s_raw = s
            s_lower = s.strip().lower()
            s_alnum = ''.join(ch.lower() for ch in s if ch.isalnum())
            s_stripped = s.replace('_', '').replace(' ', '').lower()
            return {s_raw, s_lower, s_alnum, s_stripped}

        exclude_variants = set()
        for ex in exclude:
            try:
                exclude_variants.update(_norms(ex))
            except Exception:
                continue

        # Filter out excluded columns from the list of columns to plot
        cols_to_plot = []
        for c in cols:
            try:
                c_variants = _norms(c)
                if exclude_variants.intersection(c_variants):
                    self.logger.info(f"Excluding column from class balance plots: {c}")
                    continue
            except Exception:
                pass
            cols_to_plot.append(c)

        # Determine types only for columns that will be plotted
        numeric_cols = [c for c in cols_to_plot if pd.api.types.is_numeric_dtype(data[c]) and not self._is_categorical(data[c])]
        categorical_cols = [c for c in cols_to_plot if c not in numeric_cols]

        total_plots = len(cols_to_plot)
        # layout: try square-ish grid
        ncols = int(math.ceil(math.sqrt(total_plots)))
        nrows = int(math.ceil(total_plots / ncols))

        # We'll build a single Matplotlib figure and let existing visualisers draw into axes
        # Use the factory to obtain histogram and bar_chart visualisers
        hist_viz = VisualisationFactory.get_visualisation("histogram", title=None, ylabel="Count", **kwargs)
        bar_viz = VisualisationFactory.get_visualisation("bar_chart", title=None, ylabel="Count", xticks_rotation=45, **kwargs)

        # Create a combined figure via the visualisers' plotting functions. We assume their
        # plot methods accept an `ax` kwarg to draw into an existing axis; if not, we fall back
        # to letting them return a fig and then embedding — to be defensive we'll handle both cases.
        import matplotlib.pyplot as plt
        fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(4 * ncols, 3.5 * nrows))
        # axes could be a single Axes or array
        if total_plots == 1:
            axes = [axes]
        else:
            axes = axes.flatten()

        for idx, col in enumerate(cols_to_plot):
            ax = axes[idx]
            try:
                # Call visualiser plotters and expect them to draw into the provided `ax`.
                if col in numeric_cols:
                    # Prepare series
                    ser = data[col].dropna()

                    # Apply log transform if requested for this column
                    if col in log_transform_cols:
                        import numpy as _np
                        try:
                            minv = ser.min()
                            if minv <= 0:
                                # shift to positive domain: subtract min and add 1
                                shift = 1 - minv
                                self.logger.info(f"Column '{col}' contains non-positive values; applying shift {shift} before log1p")
                                ser_trans = _np.log1p(ser + shift)
                            else:
                                ser_trans = _np.log1p(ser)
                        except Exception as e:
                            self.logger.warning(f"Failed to apply log1p to column '{col}': {e}; falling back to raw values")
                            ser_trans = ser
                    else:
                        ser_trans = ser

                    # Determine bins for this column: dict or global int
                    bins = None
                    if isinstance(hist_bins_cfg, dict):
                        bins = hist_bins_cfg.get(col, None)
                    elif isinstance(hist_bins_cfg, int):
                        bins = hist_bins_cfg

                    # Pass bins via kwargs to histogram visualiser
                    if bins is not None:
                        hist_viz.plot(data=ser_trans, ax=ax, title=col, bins=bins)
                    else:
                        hist_viz.plot(data=ser_trans, ax=ax, title=col)
                else:
                    # Pass the Series directly to the bar visualiser so it computes counts and
                    # draws categories on the x-axis and counts on the y-axis.
                    bar_viz.plot(data=data[col], ax=ax, title=col)

            except Exception as e:
                self.logger.warning(f"Failed to plot column '{col}': {e}")
                ax.text(0.5, 0.5, f"Error plotting {col}", ha="center")

        # hide any unused axes
        for j in range(total_plots, len(axes)):
            try:
                axes[j].set_visible(False)
            except Exception:
                pass

        fig.tight_layout()
        # Save the combined figure
        try:
            fig.savefig(filepath, bbox_inches='tight')
        except Exception as e:
            self.logger.error(f"Failed to save class balance figure: {e}")
            raise

        self.logger.info(f"Saved class balance figure to {filepath}")
        plt.close(fig)
        return filepath

