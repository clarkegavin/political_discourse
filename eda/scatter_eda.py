# eda/scatter_eda.py
from .base import EDAComponent
from logs.logger import get_logger
from visualisations.factory import VisualisationFactory
from reducers.factory import ReducerFactory
import os
import pandas as pd


class ScatterPlotEDA(EDAComponent):
    """
    EDA step to create a 2D/3D scatter plot using an existing visualiser (e.g. 'cluster_plot').

    Parameters (via kwargs):
      - columns: list of 2 or 3 column names to use as coordinates (preferred)
      - x: column name for X coordinate (alternative)
      - y: column name for Y coordinate
      - z: optional column name for Z coordinate
      - color_by: optional column name to use for coloring (passed as labels to visualiser)
      - viz_params: dict passed to VisualisationFactory when creating the visualiser (title, figsize, output_dir, etc.)
      - filename: output filename for the saved PNG (defaults to scatter_plot.png)
      - save_interactive: bool, if True will also save an interactive plot (html) using the visualiser
    """

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("Initialized ScatterPlotEDA")

    def run(self, data, target=None, text_field=None, save_path=None, **kwargs):
        if save_path is None:
            save_path = os.getcwd()
        if data is None or not isinstance(data, pd.DataFrame):
            raise ValueError("ScatterPlotEDA requires a pandas DataFrame")

        # reducer configuration - if provided, the EDA will reduce feature space automatically
        reducer_cfg = kwargs.get('reducer') or kwargs.get('viz_reducer') or None

        cols = kwargs.get('columns') or []
        if isinstance(cols, str):
            cols = [cols]
        try:
            cols = list(cols)
        except Exception:
            cols = []

        # If a reducer config is provided, determine feature matrix to reduce
        X_reduced = None
        if reducer_cfg:
            # Determine the input features for reducer:
            # Priority: kwargs['feature_columns'] -> kwargs['feature_selector_fn'] (not supported) -> all numeric columns
            feature_cols = kwargs.get('feature_columns')
            if feature_cols is None:
                # default to all numeric columns in the DataFrame
                feature_cols = list(data.select_dtypes(include=['number']).columns)

            if isinstance(feature_cols, str):
                feature_cols = [feature_cols]

            # Build feature matrix
            X_features = data[feature_cols].copy() if feature_cols else data.select_dtypes(include=['number']).copy()
            # instantiate reducer
            try:
                if isinstance(reducer_cfg, dict):
                    # reducer config may be {name: 'umap', params: {...}}
                    r = ReducerFactory.get_reducer(reducer_cfg)
                else:
                    # reducer_cfg could be a simple name string or a dict with 'name'
                    if isinstance(reducer_cfg, str):
                        r = ReducerFactory.create_reducer(name=reducer_cfg, **kwargs.get('reducer_params', {}))
                    else:
                        r = ReducerFactory.get_reducer(reducer_cfg)
            except Exception as e:
                self.logger.error(f"Failed to create reducer from config {reducer_cfg}: {e}")
                raise

            # perform reduction
            try:
                self.logger.info(f"Reducing dimensions using {r}")
                X_reduced = r.fit_transform(X_features)
                self.logger.info(f"Reduced shape: {getattr(X_reduced, 'shape', None)}")
            except Exception as e:
                self.logger.error(f"Reducer failed to fit_transform: {e}")
                raise

            # If reducer returned DataFrame or ndarray, proceed — no need for explicit cols
            # Set cols to None to indicate we'll use reduced output
            cols = []

        # Accept x,y,z or columns list when no reducer
        if X_reduced is None:
            if not cols:
                x = kwargs.get('x')
                y = kwargs.get('y')
                z = kwargs.get('z')
                cols = [c for c in [x, y, z] if c]

            if len(cols) not in (2, 3):
                self.logger.error("ScatterPlotEDA requires 2 or 3 columns (x,y[,z]) to plot when no reducer is provided")
                raise ValueError("ScatterPlotEDA requires 2 or 3 columns (x,y[,z]) to plot when no reducer is provided")

            # ensure columns exist
            missing = [c for c in cols if c not in data.columns]
            if missing:
                self.logger.error(f"Columns not found in DataFrame for scatter plot: {missing}")
                raise KeyError(f"Columns not found in DataFrame for scatter plot: {missing}")

        # build embedding array: either from reducer output or specified columns
        if X_reduced is not None:
            # reducer already returned DataFrame or ndarray
            if isinstance(X_reduced, pd.DataFrame):
                X = X_reduced
            else:
                # numpy array -> convert to DataFrame with generic column names
                n_dim = X_reduced.shape[1]
                cols_reduced = [f"dim_{i}" for i in range(n_dim)]
                X = pd.DataFrame(X_reduced, columns=cols_reduced)
        else:
            X = data[cols].copy()
            # coerce to numeric where possible
            for c in X.columns:
                X[c] = pd.to_numeric(X[c], errors='coerce')

        # create labels from color_by if present
        color_by = kwargs.get('color_by')
        if color_by and color_by in data.columns:
            labels = data[color_by].values
        else:
            labels = None

        viz_params = kwargs.get('viz_params') or {}
        # ensure output_dir is set so visualiser can save interactive html if required
        viz_params = dict(viz_params)
        viz_params.setdefault('output_dir', save_path)

        # get visualiser instance (default to 'cluster_plot' as requested)
        viz_name = viz_params.pop('name', 'cluster_plot')
        vis = VisualisationFactory.get_visualisation(viz_name, **viz_params)
        if vis is None:
            raise KeyError(f"Visualiser '{viz_name}' not registered in VisualisationFactory")

        # call plot: pass reduced DataFrame/array directly
        try:
            fig, ax, scatter = vis.plot(X.values if not isinstance(X, pd.DataFrame) else X, labels)
        except TypeError:
            # Some visualisers may return fig, ax, scatter for 2D/3D and others different
            res = vis.plot(X.values, labels)
            if isinstance(res, tuple) and len(res) >= 2:
                fig = res[0]
                ax = res[1]
            else:
                # fallback: wrap single output as figure
                fig = res
                ax = None
                scatter = None

        # save static image
        filename = kwargs.get('filename') or 'scatter_plot.png'
        outpath = os.path.join(save_path, filename)
        try:
            vis.save(fig, outpath)
            self.logger.info(f"Saved scatter plot to {outpath}")
        except Exception as e:
            self.logger.warning(f"Failed to save static scatter plot: {e}")

        # optionally save interactive plot
        if kwargs.get('save_interactive'):
            try:
                html_out = vis.save_interactive_plot(X.values, labels, prefix=kwargs.get('interactive_prefix', 'scatter_plot'))
                self.logger.info(f"Saved interactive scatter plot to {html_out}")
            except Exception as e:
                self.logger.warning(f"Failed to save interactive scatter plot: {e}")

        return outpath
