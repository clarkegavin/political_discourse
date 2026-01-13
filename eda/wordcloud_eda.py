from .base import EDAComponent
from logs.logger import get_logger
from visualisations.factory import VisualisationFactory
import os

class WordCloudEDA(EDAComponent):
    """
    EDA component to generate and visualize a word cloud from text data.
    """

    def __init__(self, per_class=False):
        self.per_class = per_class
        self.logger = get_logger("WordCloudEDA")
        self.logger.info("Initialized WordCloudEDA component")

    def run(self, data, target, text_field, save_path, **kwargs):
        """
        Generate and visualize a word cloud from the specified text field.

        Parameters:
        - data: The dataset containing the text data.
        - target: Not used in this component.
        - text_field: The name of the text field column to generate the word cloud from.
        - kwargs: Additional parameters for word cloud generation.

        Returns:
        - fig: The generated word cloud plot.
        """

        viz = VisualisationFactory.get_visualisation("word_cloud")

        if not self.per_class:
            # Global wordcloud
            text = " ".join(data[text_field].astype(str).tolist())
            filepath = os.path.join(save_path, "wordcloud_all.png")
            viz.plot(text=text, save_path=filepath, title="All Classes")
            return [filepath]

        # Per-class wordclouds
        filepaths = []
        for label, subset in data.groupby(target):
            text = " ".join(subset[text_field].astype(str).tolist())
            filename = f"wordcloud_class_{label}.png"
            filepath = os.path.join(save_path, filename)
            viz.plot(text=text, save_path=filepath, title=f"Class {label}")
            filepaths.append(filepath)

        return filepaths


