# visualisations/histogram.py
from .base import Visualisation
from logs.logger import get_logger

class Histogram(Visualisation):
    """
    Histogram visualisation for numeric series.
    Accepts either a pandas Series/array-like or raw numeric list.
    If provided an Axes `ax`, the plot will be drawn there; otherwise a new figure is created.
    """
    def __init__(self, title: str=None, bins: int=10, xlabel=None, ylabel=None, figsize=(8, 4), **params):
        super().__init__(title=title or "Histogram", figsize=figsize)
        self.logger = get_logger(self.__class__.__name__)
        self.bins = bins
        self.xlabel = xlabel
        self.ylabel = ylabel
        self.params = params
        self.figsize = figsize
        self.logger.info(f"Initialized Histogram visualisation with title: {title}, bins: {bins}, figsize: {figsize}")

    def plot(self, data, ax=None, title=None, **kwargs):
        import matplotlib.pyplot as plt
        import numpy as np

        self.logger.info(f"Creating Histogram visualisation for data (type={type(data)})")

        created_fig = None
        if ax is None:
            created_fig, ax = plt.subplots(figsize=self.figsize)
        else:
            created_fig = ax.figure

        # Accept pandas Series or list-like
        try:
            values = data.dropna().values if hasattr(data, 'dropna') else np.array([v for v in data if v is not None])
        except Exception:
            values = data

        # Allow callers to override bins per-call using kwargs['bins']
        bins = kwargs.pop('bins', None)
        if bins is None:
            bins = self.bins

        ax.hist(values, bins=bins, **kwargs)
        ax.set_title(title or self.title)
        if self.xlabel:
            ax.set_xlabel(self.xlabel)
        if self.ylabel:
            ax.set_ylabel(self.ylabel or 'Count')

        rotation = self.params.get('xticks_rotation')
        if rotation is not None:
            ax.tick_params(axis='x', labelrotation=rotation)

        self.logger.info("Histogram visualisation created")
        return created_fig, ax
