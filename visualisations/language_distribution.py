from visualisations.base import Visualisation
import matplotlib.pyplot as plt
import pandas as pd

class LanguageDistributionVisualisation(Visualisation):

    def __init__(self, title=None, figsize=(10, 10), **kwargs):
        super().__init__(title=title, figsize=figsize)
        self.title = title
        self.figsize = figsize
        self.xlabel = kwargs.get('xlabel', 'Language')
        self.ylabel = kwargs.get('ylabel', 'Count')


    def plot(self, data: pd.DataFrame):
        lang_counts = data['Language'].value_counts().sort_values(ascending=False)
        fig, ax = plt.subplots()
        lang_counts.plot(kind='bar', ax=ax)
        if self.title:
            ax.set_title(self.title)

        ax.set_xlabel(self.xlabel)
        ax.set_ylabel(self.ylabel)
        for i, count in enumerate(lang_counts):
            ax.text(i, count, str(count), ha='center', va='bottom')
        return fig, ax
