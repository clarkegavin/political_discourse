# visualisations/word_cloud_eda_vis.py

import matplotlib.pyplot as plt
from wordcloud import WordCloud
from logs.logger import get_logger
from .base import Visualisation


class WordCloudVisualisation(Visualisation):

    def __init__(self, title: str = None, **kwargs):
        super().__init__(title=title)
        self.logger = get_logger(self.__class__.__name__)

    def plot(self, text, **kwargs):
        """
        Expects raw text (NOT a dataframe).
        Fully decoupled from any column naming conventions.
        """


        wc_params = {
            k: v for k, v in kwargs.items()
            if k in [
                "max_words",
                "background_color",
                "colormap",
                "width",
                "height",
                "stopwords",
                "collocations"
            ]
        }

        wc = WordCloud(
            width=800,
            height=400,
            **wc_params
        ).generate(text)

        fig = plt.figure(figsize=(10, 5))
        ax = fig.add_subplot(111)

        ax.imshow(wc, interpolation="bilinear")
        ax.axis("off")
        plt.tight_layout()

        return fig, ax