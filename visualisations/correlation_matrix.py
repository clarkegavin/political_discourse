# visualisations/correlation_matrix.py
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for plotting
from .base import Visualisation
from logs.logger import get_logger
import matplotlib.pyplot as plt
import pandas as pd
import os


class CorrelationMatrix(Visualisation):
    """
    Correlation matrix visualisation using dython's associations heatmap.

    This visualisation accepts a pandas DataFrame (mixed dtypes supported) and
    uses `dython.nominal.associations` to compute and plot a correlation/association
    matrix. The plot is returned as a matplotlib Figure and Axes and can be saved
    using the base `save` helper.

    Note: dython must be installed in the environment (pip install dython).
    """

    def __init__(self, title: str = "Correlation Matrix", figsize: tuple = (12, 10),
                 output_dir: str = ".", cmap: str = "RdBu_r", annot: bool = True,
                 fmt: str = ".2f", **params):
        super().__init__(title=title, figsize=figsize)
        self.logger = get_logger(self.__class__.__name__)
        self.figsize = figsize
        self.output_dir = output_dir
        self.cmap = cmap
        self.annot = annot
        self.fmt = fmt
        self.params = params
        self.logger.info(f"Initialized CorrelationMatrix with title='{title}', figsize={figsize}, cmap={cmap}, annot={annot}")

    def build(self):
        # kept for parity with other visualisations
        self.logger.info("Built CorrelationMatrix visualisation")
        return self

    def plot(self, df: pd.DataFrame, save_path: str = None, **kwargs):
        """
        Create and optionally save a correlation/association matrix plot.

        Parameters:
            df: pandas.DataFrame
            save_path: Optional full filepath to save the figure. If not provided,
                       the function will not save automatically unless `output_dir`
                       is set and a filename is provided via kwargs['filename'].
            kwargs: Extra keyword args passed to dython.nominal.associations
                    (e.g. theil_u=True, nominal_columns=..., mark_columns=...)

        Returns:
            (fig, ax): matplotlib Figure and Axes objects
        """
        try:
            from dython.nominal import associations
        except Exception as e:
            self.logger.error("dython is not installed; cannot create correlation matrix")
            raise ImportError("dython is required for CorrelationMatrix. Install with `pip install dython`.") from e

        if df is None or not isinstance(df, pd.DataFrame):
            raise ValueError("A pandas DataFrame must be provided to CorrelationMatrix.plot()")

        # merge params: explicit attributes take precedence over kwargs
        plot_kwargs = {
            "figsize": self.figsize,
            "cmap": self.cmap,
            "annot": self.annot,
            "fmt": self.fmt,
            "title": self.title,
            "legend": False,
            "fontsize": 10,
        }
        # let user override defaults
        plot_kwargs.update(self.params)
        plot_kwargs.update(kwargs)

        self.logger.info(f"Creating correlation matrix for dataframe with shape {df.shape}")

        # dython will draw to the current matplotlib figure/axes when plot=True
        # so we clear any existing figure and call associations.
        # plt.figure(figsize=self.figsize)

        # associations will both compute and plot the correlations/associations
        # it supports mixed type data. We pass plot=True to render the heatmap.
        plt.ioff()  # turn off interactive mode to prevent display during plotting
        try:
            result = associations(df, plot=True, **plot_kwargs)
        except TypeError:
            # Some older versions of dython do not accept `fmt` or `fontsize` etc.
            # Retry with a minimal set of args.
            minimal = {k: plot_kwargs[k] for k in ["figsize", "cmap", "annot", "title"] if k in plot_kwargs}
            result = associations(df, plot=True, **minimal)

        # Newer dython versions
        if isinstance(result, dict) and "fig" in result:
            fig = result["fig"]
            ax = result.get("ax")
        else: # Older dython versions
            fig = plt.gcf()
            ax = plt.gca()
        # fig = plt.gcf()
        # ax = plt.gca()

        # Optionally save
        if save_path:
            # ensure directory exists
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            self.save(fig, save_path)
        else:
            # If user provided only a filename in kwargs, save under output_dir
            filename = kwargs.get("filename")
            if filename:
                os.makedirs(self.output_dir, exist_ok=True)
                out_path = os.path.join(self.output_dir, filename)
                self.save(fig, out_path)

        plt.close(fig)
        self.logger.info("Correlation matrix created")
        return fig, ax

