import matplotlib.pyplot as plt

from visualisations.base import Visualisation


class GroupedBarChart(Visualisation):

    def __init__(
        self,
        title="",
        figsize=(10, 6),
        **kwargs

    ):
        super().__init__(
            title,
            figsize
        )
        self.y_label = kwargs.get("y_label", "Score")


    def plot(
        self,
        data,
        x_field,
        metrics,
        **kwargs
    ):

        fig, ax = plt.subplots(
            figsize=self.figsize
        )

        x = range(
            len(data[x_field])
        )

        num_metrics = len(metrics)

        width = (
            0.8 / num_metrics
        )


        for index, metric in enumerate(metrics):

            positions = [
                i + index * width
                for i in x
            ]

            ax.bar(
                positions,
                data[metric],
                width,
                label=metric
            )


        ax.set_xticks(
            [
                i + width * (num_metrics - 1) / 2
                for i in x
            ]
        )

        ax.set_xticklabels(
            data[x_field],
            rotation=45,
            ha="right"
        )


        ax.set_ylabel(self.y_label)

        ax.set_title(
            self.title
        )

        ax.legend(
            loc="lower center",
            bbox_to_anchor=(0.5, 1.02),
            ncol=len(metrics)
        )

        fig.tight_layout(
            rect=[0, 0, 1, 0.9]
        )

        return fig, ax