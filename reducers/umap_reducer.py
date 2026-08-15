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
    def __init__(self, name='umap', **params):
        self.logger = get_logger("UMAPReducer")
        self.logger.info(f"Initializing UMAPReducer with name={name} and params={params}")
        self.name = name
        self.params = params
        self.model = None

    def build(self):
        if umap is None:
            raise RuntimeError("umap-learn is required for UMAPReducer. Install with 'pip install umap-learn'.")
        self.model = umap.UMAP(**self.params)
        self.logger.info(f"Built UMAP model with params={self.params}")
        self.logger.info(
            f"Effective UMAP parameters: {self.model.get_params()}"
        )
        return self

    def fit(self, X: Any):
        if umap is None:
            raise RuntimeError("umap-learn is required for UMAPReducer. Install with 'pip install umap-learn'.")

        if self.model is None:
            self.build()
        self.model.fit(X)
        return self

    def transform(self, X: Any):
        if self.model is None:
            # lazily create the model if fit() wasn't called
            self.model = self.build()
        return self.model.transform(X)

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

        self.model =  self.build()
        self.logger.info("UMAP model created, performing fit_transform")
        embedding  = self.model.fit_transform(X_np)
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
        self.model = None  # reset model to force re-creation with new components

