# eda/pair_scatter_eda.py
from .base import EDAComponent
from logs.logger import get_logger
from visualisations.factory import VisualisationFactory
import os
import pandas as pd
from itertools import combinations

class PairScatterEDA(EDAComponent):
    """
    EDA step that generates pairwise scatter plots for a list of numeric columns.

    Expected kwargs:
      - columns: list of columns to include (required)
      - exclude_columns: optional list of columns to exclude
      - ncols: number of columns in subplot grid
      - filename: output filename (default: pair_scatter.png)
      - viz_params: dict of parameters to pass to the PairScatter visualiser

    This EDA will generate all unique 2-combinations of the provided columns and
    use the VisualisationFactory to create a `pair_scatter` visualiser to render
    and save the grid of scatter plots.
    """

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("Initialized PairScatterEDA")

    def run(self, data, target=None, text_field=None, save_path=None, **kwargs):
        if save_path is None:
            save_path = os.getcwd()
        if data is None or not isinstance(data, pd.DataFrame):
            raise ValueError("PairScatterEDA requires a pandas DataFrame")

        cols = kwargs.get('columns') or []
        if isinstance(cols, str):
            cols = [cols]
        try:
            cols = list(cols)
        except Exception:
            cols = []

        exclude = kwargs.get('exclude_columns') or []
        if isinstance(exclude, str):
            exclude = [exclude]
        try:
            exclude = list(exclude)
        except Exception:
            exclude = []

        # filter existing columns and numeric type
        candidates = [c for c in cols if c in data.columns]
        numeric_cols = [c for c in candidates if pd.api.types.is_numeric_dtype(data[c])]

        # if user provided no columns, fallback to all numeric cols except excluded
        if not cols:
            numeric_cols = [c for c in data.select_dtypes(include=['number']).columns if c not in exclude]

        # apply excludes
        numeric_cols = [c for c in numeric_cols if c not in exclude]
        self.logger.info(f"PairScatterEDA using numeric columns: {numeric_cols}")

        if len(numeric_cols) < 2:
            self.logger.error("Need at least two numeric columns to create pairwise scatter plots")
            raise ValueError("Need at least two numeric columns to create pairwise scatter plots")

        # create all unique combinations of 2 columns
        pairs = list(combinations(numeric_cols, 2))

        # get visualiser
        viz_params = kwargs.get('viz_params') or {}
        ncols = int(kwargs.get('ncols', viz_params.get('ncols', 3)))
        filename = kwargs.get('filename') or 'pair_scatter.png'

        pair_viz = VisualisationFactory.get_visualisation('pair_scatter', title=viz_params.get('title', 'Pairwise Scatter'), figsize=viz_params.get('figsize', (12,8)), **viz_params)
        if pair_viz is None:
            raise KeyError("Visualisation 'pair_scatter' is not registered in VisualisationFactory")

        fig, axes = pair_viz.plot(data=data, pairs=pairs, ncols=ncols, title=viz_params.get('title'))

        outpath = os.path.join(save_path, filename)
        pair_viz.save(fig, outpath)
        self.logger.info(f"Saved pairwise scatter plots to {outpath}")
        return outpath

