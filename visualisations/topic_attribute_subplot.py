import math
import matplotlib.pyplot as plt

from .base import Visualisation


class TopicAttributeSubplot(Visualisation):

    def __init__(
        self,
        title=None,
        rows=2,
        cols=5,
        figsize=(18, 8),
        top_n_entities=5,
        **kwargs
    ):
        super().__init__(
            title=title,
            figsize=figsize
        )

        self.rows = rows
        self.cols = cols
        self.top_n_entities = top_n_entities

    def plot(
        self,
        data,
        top_n_entities=None,
        **kwargs
    ):

        top_n_entities = (
            top_n_entities
            if top_n_entities is not None
            else self.top_n_entities
        )

        figures = {}

        for attribute, result in data.items():

            df = result["data"]
            top_topics = result["top_topics"]

            fig, axes = plt.subplots(
                self.rows,
                self.cols,
                figsize=self.figsize,
                squeeze=False
            )

            axes = axes.flatten()

            for index, topic in enumerate(top_topics):

                ax = axes[index]

                topic_data = (
                    df[
                        df["Topic"] == topic
                    ]
                    .sort_values(
                        "count",
                        ascending=False
                    )
                    .head(top_n_entities)
                    .sort_values(
                        "count",
                        ascending=True
                    )
                )

                if topic_data.empty:
                    ax.axis("off")
                    continue

                theme = topic_data[
                    "TopicTheme"
                ].iloc[0]

                ax.barh(
                    topic_data[attribute].astype(str),
                    topic_data["count"]
                )

                # ax.set_title(
                #     theme,
                #     fontsize=10
                # )

                ax.set_title(
                    f"Topic {topic} — {theme}",
                    fontsize=10
                )

                ax.set_xlabel(
                    "Number of questions"
                )

                ax.tick_params(
                    axis="y",
                    labelsize=8
                )

                # Add count labels
                for bar in ax.patches:

                    width = bar.get_width()

                    ax.text(
                        width,
                        bar.get_y()
                        + bar.get_height() / 2,
                        f" {int(width)}",
                        va="center",
                        fontsize=8
                    )

            # Hide unused subplot axes
            for index in range(
                len(top_topics),
                len(axes)
            ):
                axes[index].axis("off")


            # fig.suptitle(
            #     f"{self.title}: {attribute}",
            #     fontsize=14
            # )

            fig.tight_layout(
                rect=[0, 0, 1, 0.96]
            )

            figures[attribute] = fig

        return figures