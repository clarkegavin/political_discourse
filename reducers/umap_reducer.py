# reducers/umap_reducer.py
from typing import Any
from logs.logger import get_logger
import pandas as pd
import numpy as np

try:
    import umap
except Exception:
    umap = None

from .base import Reducer


class UMAPReducer(Reducer):
    def __init__(self, name='umap', n_components: int = 2, random_state: int = 42, **kwargs):
        self.logger = get_logger("UMAPReducer")
        self.logger.info(f"Initializing UMAPReducer with n_components={n_components}, random_state={random_state}, kwargs={kwargs}")
        self.name = name
        if umap is None:
            self.logger.warning("umap-learn is not installed; UMAPReducer will raise on fit/transform.")
        self.n_components = n_components
        self.random_state = random_state
        self.kwargs = kwargs
        self._model = None

    def fit(self, X: Any):
        if umap is None:
            raise RuntimeError("umap-learn is required for UMAPReducer. Install with 'pip install umap-learn'.")
        self._model = umap.UMAP(n_components=self.n_components, random_state=self.random_state, **self.kwargs)
        self._model.fit(X)
        return self

    def transform(self, X: Any):
        if self._model is None:
            # lazily create the model if fit() wasn't called
            self._model = umap.UMAP(n_components=self.n_components, random_state=self.random_state, **self.kwargs)
        return self._model.transform(X)

    def fit_transform(self, X: Any):
        self.logger.info("Fitting and transforming data using UMAPReducer")
        if umap is None:
            raise RuntimeError("umap-learn is required for UMAPReducer. Install with 'pip install umap-learn'.")

        # --- Preserve index if X is a DataFrame ---
        index = X.index if hasattr(X, "index") else None

        # if X is a dataframe, convert to numpy array
        if isinstance(X, pd.DataFrame):
            self.logger.info("Input is a DataFrame, converting to numpy array")
            X_np = np.asarray(X, dtype=np.float32)
        else:
            X_np = X

        # --- Sanity checks ---
        if np.isnan(X_np).any():
            self.logger.warning("NaNs detected in UMAP input; replacing with 0")
            X_np = np.nan_to_num(X_np)

        self._model = umap.UMAP(n_components=self.n_components, random_state=self.random_state, **self.kwargs)
        self.logger.info("UMAP model created, performing fit_transform")
        embedding  = self._model.fit_transform(X)
        # convert numpy array back to DataFrame
        columns = [f"umap_{i}" for i in range(embedding.shape[1])]
        embedding_df = pd.DataFrame(
            embedding,
            index=index,
            columns=columns,
        )
        self.logger.info("UMAP fit_transform completed")
        return embedding_df


    def set_components(self, n_components: int):
        self.n_components = n_components
        self._model = None  # reset model to force re-creation with new components

