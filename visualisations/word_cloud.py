# visualisations/word_cloud.py
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from .base import Visualisation
from logs.logger import get_logger

class WordCloudChart(Visualisation):


    def __init__(self, title: str=None, output_dir=None, filename_prefix="wordcloud", **kwargs):
        super().__init__(title=title)
        self.logger = get_logger(self.__class__.__name__)
        self.output_dir = output_dir
        self.filename_prefix = filename_prefix
        self.combined_text_field_name = kwargs.get("combined_text_field_name", "__topic_input_text__")

    def plot(self, df, **kwargs):
        text = " ".join(df[self.combined_text_field_name].astype(str))

        wc = WordCloud(width=800, height=400).generate(text)

        fig = plt.figure(figsize=(10, 5))
        ax = fig.add_subplot(111)
        ax.imshow(wc, interpolation='bilinear')
        ax.axis("off")
        plt.tight_layout()

        return fig, ax

    # def save(self, fig, path):
    #     fig.savefig(path)
    #     plt.close(fig)