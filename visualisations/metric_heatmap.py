import matplotlib.pyplot as plt
import seaborn as sns

from visualisations.base import Visualisation


class MetricHeatmap(Visualisation):

    def __init__(
        self,
        title="",
        figsize=(10, 6)
    ):

        super().__init__(
            title,
            figsize
        )

        self.figsize = figsize


    def plot(
        self,
        data,
        index_field,
        metrics,
        **kwargs
    ):

        # -----------------------------
        # Select required fields
        # -----------------------------

        heatmap_data = data.set_index(
            index_field
        )[metrics]


        fig, ax = plt.subplots(
            figsize=self.figsize
        )


        sns.heatmap(
            heatmap_data,
            annot=True,
            fmt=".3f",
            cmap="viridis",
            ax=ax
        )


        ax.set_title(
            self.title
        )


        ax.set_xlabel(
            ""
        )


        ax.set_ylabel(
            ""
        )


        plt.tight_layout()


        return fig, ax