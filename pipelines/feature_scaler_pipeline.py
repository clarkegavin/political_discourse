#pipelines/feature_scaler_pipeline.py
from scalers.factory import ScalerFactory
from logs.logger import get_logger
from typing import Any, Dict
import pandas as pd

class FeatureScalerPipeline:
    """Pipeline to apply feature scaling using a specified scaler."""

    def __init__(self, scaler_config: Dict[str, Any]):
        self.logger = get_logger("FeatureScalerPipeline")
        self.scaler = ScalerFactory.get_scaler(scaler_config)
        self.columns = scaler_config.get("columns")  # optional
        self.fitted = False

        if self.scaler:
            self.logger.info(f"Initialized scaler: {scaler_config.get('name')}")
        else:
            self.logger.info("No scaler configured; proceeding without scaling.")

    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
        """Fit (if needed) and transform the dataframe."""

        if self.scaler is None:
            self.logger.info("No scaler configured; returning data unchanged.")
            return data

        if not isinstance(data, pd.DataFrame):
            raise ValueError("FeatureScalerPipeline expects a pandas DataFrame")

        data = data.copy()
        data.columns = data.columns.astype(str)

        # Ensure column names are plain strings
        data.columns = [str(c) for c in data.columns]

        # Select columns to scale
        if self.columns:
            cols_to_scale = [c for c in self.columns if c in data.columns]
        else:
            # default: numeric columns only
            cols_to_scale = data.select_dtypes(include="number").columns.tolist()

        if not cols_to_scale:
            self.logger.warning("No columns selected for scaling; returning data unchanged.")
            return data

        self.logger.info(f"Scaling columns: {cols_to_scale}")


        X = data[cols_to_scale].copy()
       #X = X.apply(pd.to_numeric, errors='coerce')
        # Ensure numeric dtype
        X = X.astype("float64")

        # Fit once
        if not self.fitted:
            self.logger.info("Fitting scaler")
            self.scaler.fit(X)
            self.fitted = True

        X_scaled = self.scaler.transform(X)

        # Reassemble DataFrame
        data_scaled = data.copy()
        data_scaled[cols_to_scale] = X_scaled

        return data_scaled

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "FeatureScalerPipeline":
        scaler_config = config.get("scaler", {})
        return cls(scaler_config)