#reducers/truncatedsvd_reducer.py
from sklearn.decomposition import TruncatedSVD
from sklearn.preprocessing import Normalizer
from sklearn.pipeline import make_pipeline
from typing import Optional
from reducers.base import Reducer
from logs.logger import get_logger
import pandas as pd

class TruncatedSVDReducer(Reducer):
    """
    Dimensionality reduction using Truncated SVD.

    Parameters:
    - n_components: int - number of components to keep
    - random_state: optional int - random seed for reproducibility
    """

    def __init__(self,  name: str = 'truncated_svd', **kwargs):

        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("Initializing TruncatedSVDReducer")
        self.model = TruncatedSVD(**kwargs)
        self.normalizer = Normalizer(copy=False)
        self.pipeline = make_pipeline(self.model, self.normalizer)
        self.name = name
        self.kwargs = kwargs

    # def build(self):
    #     """Build the Truncated SVD pipeline."""
    #     self.model = TruncatedSVD(self.kwargs)
    #     self.logger.info("Building TruncatedSVD pipeline")
    #     self.pipeline = make_pipeline(self.model, self.normalizer)
    #     return self

    def fit(self, X: pd.DataFrame):
        """Fit the Truncated SVD model to the data."""
        self.logger.info("Fitting TruncatedSVD model")
        X.columns = X.columns.astype(str)
        self.pipeline.fit(X)
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """Transform the data to reduced dimensions."""
        self.logger.info("Transforming data using TruncatedSVD")
        X.columns = X.columns.astype(str)
        X_reduced = self.pipeline.transform(X)
        return pd.DataFrame(X_reduced, index=X.index)

    def fit_transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """Fit the model and transform the data in one step."""
        self.logger.info("Fitting and transforming data using TruncatedSVD")
        # Ensure X is a dense DataFrame with string columns
        if isinstance(X, pd.DataFrame):
            X = X.copy()
            X.columns = X.columns.astype(str)
        else:
            X = pd.DataFrame(X)  # fallback if X is numpy or sparse

        # Log columns that contain NaNs
        nan_cols = X.columns[X.isna().any()].tolist()
        if nan_cols:
            self.logger.warning(f"Columns with NaN values detected before SVD: {nan_cols}")
            # Optional: log count of NaNs per column
            nan_counts = X[nan_cols].isna().sum()
            self.logger.warning(f"NaN counts per column: \n{nan_counts}")


        # Convert sparse DataFrame to dense array
        X_dense = X.sparse.to_dense() if hasattr(X, "sparse") else X.values

        X_reduced = self.pipeline.fit_transform(X_dense)

        # Return DataFrame with proper index and column names
        cols = [f"svd_{i}" for i in range(X_reduced.shape[1])]
        return pd.DataFrame(X_reduced, index=X.index, columns=cols)
