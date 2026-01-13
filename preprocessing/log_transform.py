# preprocessing/log_transform.py
from .base import Preprocessor
from logs.logger import get_logger
import pandas as pd
import numpy as np
from typing import List, Optional


class LogTransform(Preprocessor):
    """
    Apply log transforms to numeric DataFrame columns.

    Parameters:
    - columns: list of column names to transform (ignored if missing)
    - method: 'log1p' (default) or 'log' or 'log10'
    - shift: bool, if True and a column contains non-positive values, it will be shifted by (1 - min) before transform
    - suffix: optional str; if provided, transformed values are stored in a new column named `{col}{suffix}`
              if None, the original column is replaced
    """

    def __init__(self, columns: Optional[List[str]] = None, method: str = "log1p", shift: bool = True, suffix: Optional[str] = None):
        self.columns = columns or []
        self.method = method
        self.shift = bool(shift)
        self.suffix = suffix
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info(f"Initialized LogTransform columns={self.columns}, method={self.method}, shift={self.shift}, suffix={self.suffix}")

    def fit(self, X):
        # stateless
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(X, pd.DataFrame):
            self.logger.error("LogTransform.transform expects a pandas DataFrame")
            raise ValueError("LogTransform.transform expects a pandas DataFrame")

        df = X.copy()
        for col in self.columns:
            if col not in df.columns:
                self.logger.info(f"Column '{col}' not found in DataFrame - skipping log transform")
                continue

            self.logger.info(f"Applying log transform to column: {col}")
            ser = df[col]
            # Coerce to numeric
            ser_num = pd.to_numeric(ser, errors='coerce')
            if ser_num.dropna().empty:
                self.logger.warning(f"Column '{col}' has no numeric values after coercion - skipping")
                continue

            try:
                minv = ser_num.min()
            except Exception:
                minv = None

            ser_to_transform = ser_num
            shift_amount = 0.0
            if self.shift and minv is not None and minv <= 0:
                shift_amount = 1.0 - float(minv)
                self.logger.info(f"Shifting column '{col}' by {shift_amount} before log (min={minv})")
                ser_to_transform = ser_num + shift_amount

            try:
                if self.method == "log1p":
                    transformed = np.log1p(ser_to_transform)
                elif self.method == "log10":
                    transformed = np.log10(ser_to_transform)
                elif self.method == "log":
                    transformed = np.log(ser_to_transform)
                else:
                    self.logger.warning(f"Unknown log method '{self.method}' - defaulting to log1p")
                    transformed = np.log1p(ser_to_transform)
            except Exception as e:
                self.logger.exception(f"Failed to apply log transform to column '{col}': {e}")
                continue

            out_col = f"{col}{self.suffix}" if self.suffix else col
            # assign safely (cast to float to avoid dtype incompatibility with integer columns)
            # try:
            #     df.loc[:, out_col] = transformed.astype(float)
            # except Exception:
            #     df.loc[:, out_col] = transformed
            df[out_col] = transformed.astype("float64")
            self.logger.info(f"Written transformed column: {out_col}")

        return df

    def fit_transform(self, X):
        return self.fit(X).transform(X)

    def get_params(self):
        return {"columns": self.columns, "method": self.method, "shift": self.shift, "suffix": self.suffix}
