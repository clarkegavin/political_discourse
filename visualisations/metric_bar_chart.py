import matplotlib.pyplot as plt

from visualisations.base import Visualisation


class MetricBarChart(Visualisation):

    def __init__(
        self,
        title="",
        figsize=(12, 8),
        rows=None,
        cols=None,
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
        group_labels=None,
        **kwargs
    ):

        # ---------------------------------
        # Validation
        # ---------------------------------

        if parameter not in data.columns:

            raise ValueError(
                f"Parameter '{parameter}' "
                f"not found in dataframe."
            )

        missing_metrics = [
            metric
            for metric in metrics
            if metric not in data.columns
        ]

        if missing_metrics:

            raise ValueError(
                f"Metrics not found in dataframe: "
                f"{missing_metrics}"
            )

        num_metrics = len(metrics)

        if rows * cols < num_metrics:

            raise ValueError(
                f"The configured subplot grid "
                f"({rows}x{cols}) cannot contain "
                f"{num_metrics} metrics."
            )

        # ---------------------------------
        # Create figure
        # ---------------------------------

        fig, axes = plt.subplots(
            rows,
            cols,
            figsize=self.figsize,
            squeeze=False
        )

        axes = axes.flatten()

        # ---------------------------------
        # Generate metric subplots
        # ---------------------------------

        for index, metric in enumerate(metrics):

            ax = axes[index]

            grouped = (
                data
                .groupby(
                    parameter,
                    dropna=False
                )[metric]
                .mean()
                .reset_index()
            )

            # ---------------------------------
            # Sort categories
            # ---------------------------------

            grouped = grouped.sort_values(
                parameter
            )

            # ---------------------------------
            # Plot bars
            # ---------------------------------

            ax.bar(
                grouped[parameter].astype(str),
                grouped[metric]
            )

            ax.set_title(
                metric
            )

            ax.set_xlabel(
                parameter
            )

            ax.set_ylabel(
                "Score"
            )

            ax.tick_params(
                axis="x",
                rotation=45
            )


            ax.grid(
                axis="y",
                alpha=0.2
            )

        # ---------------------------------
        # Remove unused axes
        # ---------------------------------

        for index in range(
            num_metrics,
            len(axes)
        ):

            fig.delaxes(
                axes[index]
            )

        # ---------------------------------
        # Figure title
        # ---------------------------------

        if self.title:

            fig.suptitle(
                self.title
            )

        fig.tight_layout(
            rect=[
                0,
                0,
                1,
                0.96
            ]
        )

        return fig, axes