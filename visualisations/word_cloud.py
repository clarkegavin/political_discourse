# visualisations/word_cloud.py
from wordcloud import WordCloud
import matplotlib.pyplot as plt

class WordCloudChart:

    def plot(self, text, save_path, title=None, **kwargs):
        wc = WordCloud(width=800, height=400).generate(text)

        plt.figure(figsize=(10, 5))
        plt.imshow(wc, interpolation='bilinear')
        plt.axis("off")
        if title:
            plt.title(title)
        plt.tight_layout()
        plt.savefig(save_path)
        plt.close()
