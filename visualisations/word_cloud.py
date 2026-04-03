# visualisations/word_cloud.py
from wordcloud import WordCloud
import matplotlib.pyplot as plt

class WordCloudChart:

    # def plot(self, text, save_path, title=None, **kwargs):
    #     wc = WordCloud(width=800, height=400).generate(text)
    #
    #     plt.figure(figsize=(10, 5))
    #     plt.imshow(wc, interpolation='bilinear')
    #     plt.axis("off")
    #     if title:
    #         plt.title(title)
    #     plt.tight_layout()
    #     plt.savefig(save_path)
    #     plt.close()
    def __init__(self, output_dir=None, filename_prefix="wordcloud"):
        self.output_dir = output_dir
        self.filename_prefix = filename_prefix

    def plot(self, df, **kwargs):
        text = " ".join(df["__topic_input_text__"].astype(str))

        wc = WordCloud(width=800, height=400).generate(text)

        fig = plt.figure(figsize=(10, 5))
        plt.imshow(wc, interpolation='bilinear')
        plt.axis("off")
        plt.tight_layout()

        return fig

    def save(self, fig, path):
        fig.savefig(path)
        plt.close(fig)