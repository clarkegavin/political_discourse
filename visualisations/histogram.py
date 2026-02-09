# visualisations/histogram.py
from .base import Visualisation
from logs.logger import get_logger
import matplotlib.pyplot as plt
import numpy as np

class Histogram(Visualisation):
    """
    Histogram visualisation for numeric series.
    Accepts either a pandas Series/array-like or raw numeric list.
    If provided an Axes `ax`, the plot will be drawn there; otherwise a new figure is created.
    """
    def __init__(self, title: str=None, bins: int=10, xlabel=None, ylabel=None, figsize=(8, 4), **kwargs):
        super().__init__(title=title or "Histogram", figsize=figsize)
        self.logger = get_logger(self.__class__.__name__)
        self.bins = bins
        self.xlabel = xlabel
        self.ylabel = ylabel
        self.kwargs = kwargs
        self.figsize = figsize
        self.logger.info(f"Initialized Histogram visualisation with title: {title}, bins: {bins}, figsize: {figsize}")
        self.xscale = kwargs.get("xscale")
        self.yscale = kwargs.get("yscale")


    def plot(self, data, ax=None, title=None, **kwargs):

        # Merge init-time kwargs with plot-time kwargs
        params = {**self.kwargs, **kwargs}  # plot-time kwargs override init-time

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
        bins = params.pop('bins', None)
        if bins is None:
            bins = self.bins


        # Axis scaling (e.g. log)
        xscale = params.pop("xscale", self.xscale)
        yscale = params.pop("yscale", self.yscale)
        xlabel  = params.pop("xlabel", self.xlabel)
        ylabel  = params.pop("ylabel", self.ylabel)
        density = params.pop("density", False)
        self.logger.info(f"Plotting histogram with bins={bins}, density={density}, xscale='{xscale}', yscale='{yscale}'")

        if xscale:
            self.logger.info(f"Setting x-axis scale to '{xscale}'")
            ax.set_xscale(xscale)


        if yscale:
            self.logger.info(f"Setting y-axis scale to '{yscale}'")
            ax.set_yscale(yscale)

        self.logger.info(f"Received xscale='{xscale}' and yscale='{yscale}' for histogram")


        ax.hist(values, bins=bins, density=density, **params)

        ax.set_title(title or self.title)
        if xlabel:
            ax.set_xlabel(xlabel)
        if ylabel:
            ax.set_ylabel(ylabel or 'Count')

        rotation = params.get('xticks_rotation')
        if rotation is not None:
            ax.tick_params(axis='x', labelrotation=rotation)

        self.logger.info("Histogram visualisation created")
        return created_fig, ax
