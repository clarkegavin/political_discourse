import matplotlib.pyplot as plt

from .base import Visualisation


class SimilarityLinePlot(Visualisation):

    def __init__(
        self,
        title=None,
        figsize=(10, 6),
        **kwargs
    ):
        super().__init__(
            title=title,
            figsize=figsize
        )

        self.figsize = figsize

        self.source_topic_column = kwargs.get(
            "source_topic_column",
            "SourceTopicId"
        )

        self.rank_column = kwargs.get(
            "rank_column",
            "Rank"
        )

        self.similarity_column = kwargs.get(
            "similarity_column",
            "Similarity"
        )

        self.xlabel = kwargs.get(
            "xlabel",
            "Target topic rank"
        )

        self.ylabel = kwargs.get(
            "ylabel",
            "Cosine similarity"
        )

    def plot(self, data, **kwargs):

        fig, ax = plt.subplots(
            figsize=self.figsize
        )

        for source_topic_id, topic_data in data.groupby(
            self.source_topic_column
        ):

            topic_data = topic_data.sort_values(
                self.rank_column
            )

            ax.plot(
                topic_data[self.rank_column],
                topic_data[self.similarity_column],
                # marker="o",
                # markersize=3,
                linewidth=1,
                label=f"Topic {source_topic_id}"
            )

        if self.title:
            ax.set_title(self.title)

        ax.set_xlabel(self.xlabel)
        ax.set_ylabel(self.ylabel)

        #ax.legend()

        self.logger.info(
            f"Similarity line plot created with title: {self.title}"
        )

        return fig, ax