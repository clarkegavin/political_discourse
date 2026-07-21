# visualisations/boxplot.py
from .base import Visualisation
from logs.logger import get_logger
import matplotlib.pyplot as plt
import pandas as pd

class BoxPlot(Visualisation):
    """
    Simple BoxPlot visualisation for a numeric pandas Series.
    """
    def __init__(self, title: str = None, figsize=(6,4), ylabel: str = None,
                 x_column: str = None,
                 y_column: str = None,
                 **params):
        super().__init__(title=title or "Box Plot", figsize=figsize)
        self.logger = get_logger(self.__class__.__name__)
        self.figsize = figsize
        self.params = params
        self.ylabel = ylabel
        self.yscale = params.get("yscale", None)
        self.x_column = x_column
        self.y_column = y_column

    def plot(self, data, ax=None, title=None, **kwargs):
        """
        Plot a single boxplot from a pandas Series or numeric array-like.
        Returns (fig, ax).
        """
        created_fig = None
        if ax is None:
            created_fig, ax = plt.subplots(figsize=self.figsize)
        else:
            created_fig = ax.figure

        if isinstance(data, pd.DataFrame) and self.x_column and self.y_column:
            return self._plot_grouped_boxplot(
                data,
                ax,
                title
            )

        # Accept Series or list-like
        try:
            ser = data.dropna() if hasattr(data, 'dropna') else pd.Series([v for v in data if v is not None])
        except Exception:
            ser = pd.Series(data)

        # Coerce to numeric if possible
        try:
            ser_num = pd.to_numeric(ser, errors='coerce').dropna()
        except Exception:
            ser_num = ser

        if len(ser_num) == 0:
            ax.text(0.5, 0.5, 'No numeric data', ha='center')
            return created_fig, ax

        ax.boxplot(ser_num, vert=True, patch_artist=True)
        ax.set_title(title or self.title)
        if self.ylabel:
            ax.set_ylabel(self.ylabel)



        self.logger.info(f"Setting yscale to {self.yscale}")
        if self.yscale:
            ax.set_yscale(self.yscale)

        return created_fig, ax

    def _plot_grouped_boxplot(self, data, ax, title):
        """
        Plot grouped boxplots from dataframe columns.
        """

        required = [
            self.x_column,
            self.y_column
        ]

        missing = [
            c for c in required
            if c not in data.columns
        ]

        if missing:
            raise ValueError(
                f"Missing columns for grouped boxplot: {missing}"
            )

        grouped_data = []

        labels = []

        for category, group in data.groupby(self.x_column):

            values = (
                pd.to_numeric(
                    group[self.y_column],
                    errors="coerce"
                )
                .dropna()
            )

            if len(values) > 0:
                grouped_data.append(values)
                labels.append(category)

        if not grouped_data:
            ax.text(
                0.5,
                0.5,
                "No numeric data",
                ha="center"
            )
            return ax.figure, ax

        ax.boxplot(
            grouped_data,
            labels=labels,
            vert=True,
            patch_artist=True
        )

        ax.set_title(title or self.title)

        if self.ylabel:
            ax.set_ylabel(self.ylabel)

        if self.yscale:
            ax.set_yscale(self.yscale)

        return ax.figure, ax