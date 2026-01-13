# pipelines/target_feature_pipeline.py
from sklearn.preprocessing import LabelEncoder
from logs.logger import get_logger
from pipelines.base import Pipeline
import pandas as pd
from typing import Optional

class TargetFeaturePipeline(Pipeline):
    """
    Pipeline to encode the target feature.
    """
    def __init__(self, target_column: str):
        self.target_column = target_column
        self.encoder = LabelEncoder()
        self.logger = get_logger(self.__class__.__name__)
        self.fitted = False

    def fit(self, y: pd.Series):
        """Fit the encoder on the training target."""
        self.logger.info(f"Fitting target encoder on column '{self.target_column}'")
        self.encoder.fit(y)
        self.fitted = True
        return self

    def transform(self, y: pd.Series) -> pd.Series:
        """Transform target using fitted encoder."""
        if not self.fitted:
            raise RuntimeError("Target encoder not fitted yet. Call fit() first.")
        self.logger.info(f"Transforming target column '{self.target_column}'")
        data =  pd.Series(self.encoder.transform(y), index=y.index, name=y.name)
        return data

    def fit_transform(self, y: pd.Series) -> pd.Series:
        """Fit the target encoder on y and return transformed y."""
        self.logger.info(f"Fitting and transforming target column '{self.target_column}'")
        self.fit(y)
        return self.transform(y)

    def execute(self, y: pd.Series, fit: bool = True) -> pd.Series:
        """Run fit_transform (for training) or transform (for test)."""
        if fit:
            return self.fit_transform(y)
        else:
            return self.transform(y)
