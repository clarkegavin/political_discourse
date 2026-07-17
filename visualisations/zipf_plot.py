import matplotlib.pyplot as plt
from visualisations.base import Visualisation
from logs.logger import get_logger

class ZipfPlot(Visualisation):

    def __init__(self, title: str = None, figsize=(6,4), ylabel: str = "Frequency (log scale)", **params):
        super().__init__(title=title, figsize=figsize)
        self.logger = get_logger(self.__class__.__name__)
        self.figsize = figsize
        self.params = params
        self.ylabel = ylabel

    def plot(self, data, log_scale=True, filename=None, **kwargs):
        self.logger.info("Generating Zipf plot.")
        data = data.sort_values(by="rank")
        fig, ax = plt.subplots(figsize=kwargs.get("figsize", (10, 6)))
        ax.scatter(data["rank"], data["frequency"], alpha=kwargs.get("alpha", 0.7),
                   marker=kwargs.get("marker", "o"), s=kwargs.get("s", 10))

        if log_scale:
            ax.set_xscale("log")
            ax.set_yscale("log")
            ax.set_xlabel("Rank (log scale)")
            ax.set_ylabel("Frequency (log scale)")
        else:
            ax.set_xlabel("Rank")
            ax.set_ylabel("Frequency")

        #ax.set_title
        # if filename:
        #     plt.savefig(filename)
        return fig, ax
