#visualisations/line_plot.py
import matplotlib.pyplot as plt
from .base import Visualisation
import matplotlib.dates as mdates

class LinePlot(Visualisation):
    def __init__(self, title=None,  figsize=(10,10), **kwargs):
        super().__init__(title=title, figsize=figsize)
        self.include_mean = kwargs.get('include_mean', False)
        self.mean_line_colour = kwargs.get('mean_line_colour', 'black')
        self.xlabel = kwargs.get('xlabel', 'Period')
        self.ylabel = kwargs.get('ylabel', 'Count')
        self.title = title
        self.x_axis_frequency = kwargs.get(
            'x_axis_frequency',
            None
        )

    def plot(self, data):
        fig, ax = plt.subplots(figsize=(10, 6))

        ax.plot(
            data['period'],
            data['count'],
            label='Count'
        )

        if self.x_axis_frequency == "monthly":
            ax.xaxis.set_major_locator(
                mdates.MonthLocator(interval=1)
            )

        plt.xticks(rotation=45)

        if self.include_mean:
            mean_value = data['count'].mean()
            ax.axhline(
                mean_value,
                color=self.mean_line_colour,
                linestyle='--',
                label=f'Mean: {mean_value:.2f}'
            )

        if self.title:
            ax.set_title(self.title)
        ax.set_xlabel(self.xlabel)
        ax.set_ylabel(self.ylabel)
        ax.legend()

        self.logger.info(
            f"Line plot created with title: {self.title}"
        )

        return fig, ax
