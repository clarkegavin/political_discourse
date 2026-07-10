from visualisations.base import Visualisation
import matplotlib.pyplot as plt
import pandas as pd

class LanguageByColumnVisualisation(Visualisation):

    def __init__(self, title=None, figsize=(10, 10), **kwargs):
        super().__init__(title=title, figsize=figsize)
        self.title = title
        self.figsize = figsize
        self.xlabel = kwargs.get('xlabel', 'Language')
        self.ylabel = kwargs.get('ylabel', 'Count')

    def plot(self, data: pd.DataFrame):
        grouped = data.groupby(['ColumnName', 'Language']).size().unstack(fill_value=0)
        fig, ax = plt.subplots()
        grouped.plot(kind='bar', stacked=True, ax=ax)
        if self.title:
            ax.set_title(self.title)
        ax.set_xlabel(self.xlabel)
        ax.set_ylabel(self.ylabel)
        return fig, ax
