import math
import matplotlib.pyplot as plt
import pandas as pd

from visualisations.base import Visualisation


class MetricLineChart(Visualisation):

    def __init__(
        self,
        title="",
        figsize=(12, 8),
        rows = None,
        cols = None,
    ):
        super().__init__(
            title,
            figsize
        )

        self.figsize = figsize
        self.rows = rows
        self.cols = cols


    def plot(
        self,
        data,
        parameter,
        metrics,
        rows=2,
        cols=3,
        **kwargs
    ):

        num_metrics = len(metrics)

        fig, axes = plt.subplots(
            rows,
            cols,
            figsize=self.figsize,
            squeeze=False
        )

        axes = axes.flatten()

        # Sort data before grouping
        data = self._sort_parameter(
            data,
            parameter
        )

        for index, metric in enumerate(metrics):

            ax = axes[index]

            grouped = (
                data
                .groupby(parameter)[metric]
                .mean()
                .reset_index()
            )

            # Sort x-axis values
            try:
                grouped[parameter] = pd.to_numeric(
                    grouped[parameter]
                )
                grouped = grouped.sort_values(
                    parameter
                )
            except ValueError:
                grouped = grouped.sort_values(
                    parameter
                )


            ax.plot(
                grouped[parameter],
                grouped[metric],
                marker="o",
                linewidth=2
            )


            ax.set_title(metric)

            ax.set_xlabel(parameter)

            ax.set_ylabel(
                "Score"
            )


            # rotate categorical labels
            if not pd.api.types.is_numeric_dtype(
                grouped[parameter]
            ):
                ax.tick_params(
                    axis="x",
                    rotation=45
                )


        # remove unused axes
        for index in range(
            num_metrics,
            len(axes)
        ):
            fig.delaxes(
                axes[index]
            )


        fig.suptitle(
            self.title
        )


        fig.tight_layout()

        return fig, axes

    def _sort_parameter(
            self,
            data,
            parameter
    ):

        try:
            data[parameter] = pd.to_numeric(
                data[parameter]
            )
        except ValueError:
            pass

        return data.sort_values(
            parameter
        )