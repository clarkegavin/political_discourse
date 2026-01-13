# visualisations/pair_scatter.py
from .base import Visualisation
from logs.logger import get_logger
import matplotlib.pyplot as plt
import pandas as pd
import math

class PairScatter(Visualisation):
    """
    Create a grid of 2D scatter plots for specified column pairs.

    plot(data=DataFrame, pairs=[(x,y), ...], ncols=3, figsize=None, marker='o', alpha=0.7, s=10, cmap=None)
    """

    def __init__(self, title: str = "Pairwise Scatter", figsize=(12, 8), **params):
        super().__init__(title=title, figsize=figsize)
        self.logger = get_logger(self.__class__.__name__)
        self.params = params
        self.figsize = figsize

    def plot(self, data, pairs=None, ncols: int = 3, figsize=None, title=None, **kwargs):
        if figsize is None:
            figsize = self.figsize

        if data is None or not isinstance(data, pd.DataFrame):
            raise ValueError("PairScatter requires a pandas DataFrame as `data`")

        if pairs is None:
            raise ValueError("No column pairs provided to PairScatter.plot")

        try:
            pairs = list(pairs)
        except Exception:
            raise ValueError("`pairs` must be an iterable of (x_col, y_col) tuples")

        total = len(pairs)
        if total == 0:
            raise ValueError("No column pairs to plot")

        ncols = int(ncols) if ncols is not None else 3
        nrows = int(math.ceil(total / ncols))

        fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=figsize)

        # normalize axes to 1D list for easy iteration
        if total == 1:
            axes_list = [axes]
        else:
            axes_list = axes.flatten()

        marker = kwargs.get('marker', self.params.get('marker', 'o'))
        alpha = kwargs.get('alpha', self.params.get('alpha', 0.7))
        s = kwargs.get('s', self.params.get('s', 10))
        cmap = kwargs.get('cmap', self.params.get('cmap', None))

        for i, (xcol, ycol) in enumerate(pairs):
            ax = axes_list[i]
            if xcol not in data.columns or ycol not in data.columns:
                ax.text(0.5, 0.5, f"Missing cols: {xcol}, {ycol}", ha='center')
                continue

            x = data[xcol]
            y = data[ycol]

            # attempt to coerce to numeric where appropriate, but plot as-is if not numeric
            try:
                x_num = pd.to_numeric(x, errors='coerce')
                y_num = pd.to_numeric(y, errors='coerce')
            except Exception:
                x_num, y_num = x, y

            valid = ~(x_num.isna() | y_num.isna()) if hasattr(x_num, 'isna') else None
            if valid is not None:
                x_plot = x_num[valid]
                y_plot = y_num[valid]
            else:
                x_plot = x_num
                y_plot = y_num

            ax.scatter(x_plot, y_plot, c=None if cmap is None else cmap, marker=marker, alpha=alpha, s=s)
            ax.set_xlabel(xcol)
            ax.set_ylabel(ycol)
            ax.set_title(title or f"{xcol} vs {ycol}")

        # hide unused axes
        for j in range(total, len(axes_list)):
            try:
                axes_list[j].set_visible(False)
            except Exception:
                pass

        fig.tight_layout()
        return fig, axes
