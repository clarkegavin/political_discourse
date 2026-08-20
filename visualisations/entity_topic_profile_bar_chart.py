import math
import matplotlib.pyplot as plt

from .base import Visualisation


class EntityTopicProfileBarChart(Visualisation):
    """
    Horizontal bar chart for entity-level LLM topic profiles.

    Supports:

    - subplot:
        one subplot per entity

    - combined:
        all selected entities and their top topics in
        a single horizontal bar chart
    """

    def __init__(
        self,
        title=None,
        mode="subplot",
        rows=2,
        cols=5,
        figsize=(18, 10),
        max_label_length=30,
        show_count=True,
        show_share=True,
        **kwargs
    ):
        super().__init__(
            title=title,
            figsize=figsize
        )

        self.mode = mode
        self.rows = rows
        self.cols = cols
        self.max_label_length = max_label_length
        self.show_count = show_count
        self.show_share = show_share

    # --------------------------------------------------
    # Helpers
    # --------------------------------------------------

    def _truncate_label(
        self,
        label
    ):
        label = str(label)

        if (
            self.max_label_length is None
            or len(label) <= self.max_label_length
        ):
            return label

        if self.max_label_length <= 3:
            return label[
                :self.max_label_length
            ]

        return (
            label[
                :self.max_label_length - 3
            ]
            + "..."
        )

    def _format_annotation(
        self,
        count,
        share
    ):
        parts = []

        if self.show_count:
            parts.append(
                f"{int(count)}"
            )

        if self.show_share:
            parts.append(
                f"{share * 100:.1f}%"
            )

        if not parts:
            return ""

        return " (" + ", ".join(parts) + ")"

    def _prepare_entity_data(
        self,
        result,
        entity
    ):
        df = result["data"]

        attribute = (
            df[entity].name
            if isinstance(
                df[entity],
                pd.Series
            )
            else None
        )

    # --------------------------------------------------
    # Plot
    # --------------------------------------------------

    def plot(
        self,
        data,
        **kwargs
    ):

        mode = kwargs.get(
            "mode",
            self.mode
        )

        if mode not in [
            "subplot",
            "combined"
        ]:
            raise ValueError(
                f"Unsupported mode '{mode}'. "
                "Expected 'subplot' or 'combined'."
            )

        if mode == "subplot":
            return self._plot_subplots(
                data,
                **kwargs
            )

        return self._plot_combined(
            data,
            **kwargs
        )

    # --------------------------------------------------
    # Subplot mode
    # --------------------------------------------------

    def _plot_subplots(
        self,
        data,
        **kwargs
    ):

        figures = {}

        for attribute, result in data.items():

            df = result["data"]
            top_entities = result["top_entities"]

            if df.empty:
                continue

            rows = kwargs.get(
                "rows",
                self.rows
            )

            cols = kwargs.get(
                "cols",
                self.cols
            )

            fig, axes = plt.subplots(
                rows,
                cols,
                figsize=self.figsize,
                squeeze=False
            )

            axes = axes.flatten()

            for index, entity in enumerate(
                top_entities
            ):

                if index >= len(axes):
                    break

                ax = axes[index]

                entity_data = (
                    df[
                        df[attribute] == entity
                    ]
                    .sort_values(
                        "topic_count",
                        ascending=True
                    )
                )

                if entity_data.empty:
                    ax.axis("off")
                    continue

                labels = [
                    self._truncate_label(
                        value
                    )
                    for value in entity_data[
                        self._get_theme_column(
                            entity_data,
                            attribute
                        )
                    ]
                ]

                bars = ax.barh(
                    labels,
                    entity_data[
                        "topic_count"
                    ]
                )

                # --------------------------------------------------
                # Entity title
                # --------------------------------------------------

                total_questions = (
                    entity_data[
                        "total_questions"
                    ]
                    .iloc[0]
                )

                distinct_themes = (
                    entity_data[
                        "distinct_themes"
                    ]
                    .iloc[0]
                )

                ax.set_title(
                    (
                        f"{entity}\n"
                        f"{int(total_questions):,} questions; "
                        f"{int(distinct_themes):,} topics"
                    ),
                    fontsize=10
                )

                ax.set_xlabel(
                    "Number of questions"
                )

                ax.tick_params(
                    axis="y",
                    labelsize=8
                )

                # --------------------------------------------------
                # Count / share annotations
                # --------------------------------------------------

                for bar, (_, row) in zip(
                    bars,
                    entity_data.iterrows()
                ):

                    annotation = (
                        self._format_annotation(
                            row["topic_count"],
                            row["topic_share"]
                        )
                    )

                    if annotation:

                        ax.text(
                            bar.get_width(),
                            (
                                bar.get_y()
                                + bar.get_height() / 2
                            ),
                            f" {annotation}",
                            va="center",
                            fontsize=8
                        )

            # --------------------------------------------------
            # Hide unused axes
            # --------------------------------------------------

            for index in range(
                len(top_entities),
                len(axes)
            ):
                axes[index].axis("off")

            if self.title:
                fig.suptitle(
                    self.title,
                    fontsize=14
                )

            fig.tight_layout(
                rect=[
                    0,
                    0,
                    1,
                    0.96
                ]
            )

            figures[attribute] = fig

        return figures

    # --------------------------------------------------
    # Combined mode
    # --------------------------------------------------

    def _plot_combined(
            self,
            data,
            **kwargs
    ):

        figures = {}

        for attribute, result in data.items():

            df = result["data"]
            top_entities = result["top_entities"]
            theme_column = result["topic_theme_field"]

            if df.empty:
                continue

            # --------------------------------------------------
            # Build grouped rows
            #
            # Each entity gets:
            #   1 heading row
            #   N topic rows
            # --------------------------------------------------

            plot_rows = []

            for entity in top_entities:

                entity_data = (
                    df[
                        df[attribute] == entity
                        ]
                    .sort_values(
                        "topic_rank",
                        ascending=True
                    )
                )

                if entity_data.empty:
                    continue

                # Entity-level information
                total_questions = (
                    entity_data[
                        "total_questions"
                    ].iloc[0]
                )

                distinct_themes = (
                    entity_data[
                        "distinct_themes"
                    ].iloc[0]
                )

                # --------------------------------------------------
                # Entity heading
                # --------------------------------------------------

                plot_rows.append(
                    {
                        "type": "entity",
                        "entity": entity,
                        "total_questions": total_questions,
                        "distinct_themes": distinct_themes,
                    }
                )

                # --------------------------------------------------
                # Topics belonging to entity
                # --------------------------------------------------

                for _, row in entity_data.iterrows():
                    plot_rows.append(
                        {
                            "type": "topic",
                            "entity": entity,
                            "theme": row[
                                theme_column
                            ],
                            "count": row[
                                "topic_count"
                            ],
                            "share": row[
                                "topic_share"
                            ],
                        }
                    )

            if not plot_rows:
                continue

            # --------------------------------------------------
            # Reverse so first entity appears at the top
            # --------------------------------------------------

            plot_rows = list(
                reversed(plot_rows)
            )

            # --------------------------------------------------
            # Calculate figure height
            # --------------------------------------------------

            fig_height = max(
                self.figsize[1],
                len(plot_rows) * 0.35
            )

            fig, ax = plt.subplots(
                figsize=(
                    self.figsize[0],
                    fig_height
                )
            )

            # --------------------------------------------------
            # Plot positions
            # --------------------------------------------------

            positions = list(
                range(len(plot_rows))
            )

            # --------------------------------------------------
            # Draw topic bars only
            # --------------------------------------------------

            topic_positions = []
            topic_values = []

            for position, row in zip(
                    positions,
                    plot_rows
            ):

                if row["type"] == "topic":
                    topic_positions.append(
                        position
                    )

                    topic_values.append(
                        row["count"]
                    )

            bars = ax.barh(
                topic_positions,
                topic_values
            )

            # --------------------------------------------------
            # Y-axis labels
            #
            # Entity headings have no bar.
            # Topic labels are indented visually.
            # --------------------------------------------------

            labels = []

            for row in plot_rows:

                if row["type"] == "entity":

                    labels.append(
                        row["entity"]
                    )

                else:

                    labels.append(
                        "    "
                        + self._truncate_label(
                            row["theme"]
                        )
                    )

            ax.set_yticks(
                positions
            )

            ax.set_yticklabels(
                labels
            )

            # --------------------------------------------------
            # Style entity headings
            # --------------------------------------------------

            for tick, row in zip(
                    ax.get_yticklabels(),
                    plot_rows
            ):

                if row["type"] == "entity":

                    tick.set_fontweight(
                        "bold"
                    )

                    tick.set_fontsize(
                        10
                    )

                else:

                    tick.set_fontsize(
                        8
                    )

            # --------------------------------------------------
            # Add entity information to headings
            # --------------------------------------------------

            for position, row in zip(
                    positions,
                    plot_rows
            ):

                if row["type"] != "entity":
                    continue

                entity_label = (
                    f"{row['entity']}  "
                    f"({int(row['total_questions']):,} "
                    f"questions; "
                    f"{int(row['distinct_themes']):,} "
                    f"topics)"
                )

                # Replace the tick label with the richer heading
                ax.get_yticklabels()[
                    positions.index(position)
                ].set_text(
                    entity_label
                )

            # --------------------------------------------------
            # Re-apply labels because set_text() on tick labels
            # does not always update the rendered labels.
            # --------------------------------------------------

            final_labels = []

            for row in plot_rows:

                if row["type"] == "entity":

                    final_labels.append(
                        (
                            f"{row['entity']}  "
                            f"({int(row['total_questions']):,} "
                            f"questions; "
                            f"{int(row['distinct_themes']):,} "
                            f"topics)"
                        )
                    )

                else:

                    final_labels.append(
                        "    "
                        + self._truncate_label(
                            row["theme"]
                        )
                    )

            ax.set_yticklabels(
                final_labels
            )

            # --------------------------------------------------
            # Annotate bars
            # --------------------------------------------------

            topic_index = 0

            for row in plot_rows:

                if row["type"] != "topic":
                    continue

                bar = bars[topic_index]

                annotation = (
                    self._format_annotation(
                        row["count"],
                        row["share"]
                    )
                )

                if annotation:
                    ax.text(
                        bar.get_width(),
                        (
                                bar.get_y()
                                + bar.get_height() / 2
                        ),
                        f" {annotation}",
                        va="center",
                        fontsize=8
                    )

                topic_index += 1

            # --------------------------------------------------
            # Remove y-axis tick marks
            # --------------------------------------------------

            ax.tick_params(
                axis="y",
                length=0
            )

            ax.set_xlabel(
                "Number of questions"
            )

            # --------------------------------------------------
            # Add some vertical space between entities
            # --------------------------------------------------

            for position, row in zip(
                    positions,
                    plot_rows
            ):

                if row["type"] == "entity":
                    ax.axhline(
                        position - 0.5,
                        linewidth=0.8,
                        alpha=0.3
                    )

            # --------------------------------------------------
            # Title
            # --------------------------------------------------

            if self.title:
                ax.set_title(
                    self.title,
                    fontsize=14
                )

            fig.tight_layout()

            figures[attribute] = fig

        return figures

    # --------------------------------------------------
    # Theme column helper
    # --------------------------------------------------

    def _get_theme_column(
        self,
        df,
        attribute
    ):
        """
        Identify the TopicTheme column without hard-coding
        the configured field name into the visualisation.
        """

        excluded = {
            attribute,
            "topic_count",
            "total_questions",
            "topic_share",
            "topic_rank",
            "distinct_themes",
        }

        candidates = [
            column
            for column in df.columns
            if column not in excluded
        ]

        if not candidates:
            raise ValueError(
                "Could not identify TopicTheme "
                "column in visualisation data."
            )

        return candidates[0]