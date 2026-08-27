import matplotlib.pyplot as plt

from .base import Visualisation


class RankedHorizontalBarChart(Visualisation):

    def __init__(
        self,
        value_column,
        label_column,
        top_n=20,
        xlabel=None,
        ylabel=None,
        title=None,
        figsize=(10, 8),
        annotation_columns=None,
        annotation_formats=None,
        **kwargs
    ):

        super().__init__(
            title=title,
            figsize=figsize
        )

        self.value_column = value_column
        self.label_column = label_column
        self.top_n = top_n
        self.xlabel = xlabel
        self.ylabel = ylabel
        self.figsize = figsize

        self.annotation_columns = (
            annotation_columns or []
        )

        self.annotation_formats = (
            annotation_formats or {}
        )

    def plot(self, data, **kwargs):

        value_column = kwargs.get(
            "value_column",
            self.value_column
        )

        label_column = kwargs.get(
            "label_column",
            self.label_column
        )

        top_n = kwargs.get(
            "top_n",
            self.top_n
        )

        xlabel = kwargs.get(
            "xlabel",
            self.xlabel
        )

        ylabel = kwargs.get(
            "ylabel",
            self.ylabel
        )

        title = kwargs.get(
            "title",
            self.title
        )

        figsize = kwargs.get(
            "figsize",
            self.figsize
        )

        annotation_columns = kwargs.get(
            "annotation_columns",
            self.annotation_columns
        )

        annotation_formats = kwargs.get(
            "annotation_formats",
            self.annotation_formats
        )

        # --------------------------------------------------------------
        # Validate
        # --------------------------------------------------------------

        if value_column not in data.columns:
            raise ValueError(
                f"Column '{value_column}' "
                f"not found in data."
            )

        if label_column not in data.columns:
            raise ValueError(
                f"Column '{label_column}' "
                f"not found in data."
            )

        # --------------------------------------------------------------
        # Rank and select top N
        # --------------------------------------------------------------

        plot_data = (
            data
            .sort_values(
                value_column,
                ascending=False
            )
            .head(top_n)
            .copy()
        )

        # Reverse so highest value appears at top
        plot_data = plot_data.iloc[::-1]

        # --------------------------------------------------------------
        # Plot
        # --------------------------------------------------------------

        fig, ax = plt.subplots(
            figsize=figsize
        )

        bars = ax.barh(
            plot_data[label_column],
            plot_data[value_column]
        )

        # --------------------------------------------------------------
        # Labels
        # --------------------------------------------------------------

        if xlabel:
            ax.set_xlabel(xlabel)

        if ylabel:
            ax.set_ylabel(ylabel)

        if title:
            ax.set_title(title)

        # --------------------------------------------------------------
        # Annotations
        # --------------------------------------------------------------

        maximum = plot_data[
            value_column
        ].max()

        for bar, (_, row) in zip(
            bars,
            plot_data.iterrows()
        ):

            parts = []

            for column in annotation_columns:

                if column not in row.index:
                    continue

                value = row[column]

                fmt = annotation_formats.get(
                    column,
                    "{}"
                )

                parts.append(
                    fmt.format(value)
                )

            annotation = " | ".join(parts)

            ax.text(
                bar.get_width()
                + maximum * 0.01,
                bar.get_y()
                + bar.get_height() / 2,
                annotation,
                va="center",
                fontsize=9
            )

        # Give annotation space
        ax.set_xlim(
            0,
            maximum * 1.35
        )

        plt.tight_layout()

        return fig, ax