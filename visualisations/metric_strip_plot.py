import matplotlib.pyplot as plt
import numpy as np

from visualisations.base import Visualisation


class MetricStripPlot(Visualisation):

    def __init__(
        self,
        title="",
        figsize=(12, 8)
    ):
        super().__init__(
            title,
            figsize
        )

    def plot(
        self,
        data,
        group_field,
        metrics,
        rows=2,
        cols=3,
        central_tendency=None,
        jitter=0.08,
        group_labels=None,
        **kwargs
    ):

        if group_field not in data.columns:
            raise ValueError(
                f"Group field '{group_field}' "
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

        if central_tendency not in (
            None,
            "mean",
            "median"
        ):
            raise ValueError(
                "central_tendency must be one of "
                "'mean', 'median' or None."
            )

        if rows * cols < len(metrics):
            raise ValueError(
                f"Grid of {rows}x{cols} cannot display "
                f"{len(metrics)} metrics."
            )

        # ---------------------------------
        # Determine groups
        # ---------------------------------

        groups = (
            data[group_field]
            .dropna()
            .unique()
            .tolist()
        )

        # ---------------------------------
        # Resolve display labels
        # ---------------------------------

        if group_labels is None:
            display_labels = [
                str(group)
                for group in groups
            ]

        else:

            display_labels = [
                group_labels.get(
                    group,
                    str(group)
                )
                for group in groups
            ]

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
        # Create consistent colours
        # ---------------------------------

        cmap = plt.get_cmap(
            "tab10"
        )

        group_colours = {
            group: cmap(index % 10)
            for index, group in enumerate(groups)
        }

        # ---------------------------------
        # Plot metrics
        # ---------------------------------

        for metric_index, metric in enumerate(metrics):

            ax = axes[metric_index]

            for group_index, group in enumerate(groups):

                group_data = data[
                    data[group_field] == group
                ][metric].dropna()

                if group_data.empty:
                    continue

                values = group_data.to_numpy()

                x_values = np.full(
                    len(values),
                    group_index,
                    dtype=float
                )

                if jitter:
                    x_values += np.random.uniform(
                        -jitter,
                        jitter,
                        size=len(values)
                    )

                ax.scatter(
                    x_values,
                    values,
                    alpha=0.65,
                    s=25,
                    color=group_colours[group],
                    label=display_labels[group_index]
                )

                # -----------------------------
                # Optional central tendency
                # -----------------------------

                if central_tendency == "median":

                    centre = np.median(
                        values
                    )

                    ax.scatter(
                        group_index,
                        centre,
                        marker="D",
                        s=55,
                        color=group_colours[group],
                        edgecolor="black",
                        linewidth=0.8,
                        zorder=10
                    )

                elif central_tendency == "mean":

                    centre = np.mean(
                        values
                    )

                    ax.scatter(
                        group_index,
                        centre,
                        marker="_",
                        s=180,
                        linewidths=3,
                        color=group_colours[group],
                        zorder=5
                    )

            # -----------------------------
            # Subplot formatting
            # -----------------------------

            ax.set_title(
                metric
            )

            ax.set_xticks(
                range(len(groups))
            )

            ax.set_xticklabels(
                display_labels
            )

            ax.set_ylabel(
                "Score"
            )

            ax.grid(
                axis="y",
                alpha=0.2
            )

        # ---------------------------------
        # Hide unused axes
        # ---------------------------------

        for index in range(
            len(metrics),
            len(axes)
        ):

            axes[index].set_visible(
                False
            )

        # ---------------------------------
        # Figure-level legend
        # ---------------------------------

        legend_handles = []

        for index, group in enumerate(groups):

            handle = plt.Line2D(
                [0],
                [0],
                marker="o",
                linestyle="",
                markersize=7,
                markerfacecolor=group_colours[group],
                markeredgecolor=group_colours[group],
                label=display_labels[index]
            )

            legend_handles.append(
                handle
            )

        if legend_handles:

            fig.legend(
                handles=legend_handles,
                loc="lower center",
                bbox_to_anchor=(0.5, 0.0),
                ncol=len(legend_handles),
                frameon=False
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
                0.08,
                1,
                0.96
            ]
        )

        return fig, axes