# visualisations/similarity_histogram.py

import matplotlib.pyplot as plt

from .base import Visualisation


class SimilarityHistogram(Visualisation):

    def __init__(
        self,
        title=None,
        figsize=(10, 6),
        bins=20,
        xlabel="Cosine similarity",
        ylabel="Number of parliamentary topics",
        **kwargs
    ):

        super().__init__(
            title=title,
            figsize=figsize
        )

        self.bins = bins
        self.xlabel = xlabel
        self.ylabel = ylabel

    def plot(
        self,
        data,
        bins=None,
        title=None,
        xlabel=None,
        ylabel=None,
        **kwargs
    ):

        bins = (
            bins
            if bins is not None
            else self.bins
        )

        title = (
            title
            if title is not None
            else self.title
        )

        xlabel = (
            xlabel
            if xlabel is not None
            else self.xlabel
        )

        ylabel = (
            ylabel
            if ylabel is not None
            else self.ylabel
        )

        fig, ax = plt.subplots(
            figsize=self.figsize
        )

        ax.hist(
            data["BestSimilarity"],
            bins=bins
        )

        ax.set_title(
            title
        )

        ax.set_xlabel(
            xlabel
        )

        ax.set_ylabel(
            ylabel
        )

        ax.grid(
            axis="y",
            alpha=0.25
        )

        fig.tight_layout()

        return fig, ax