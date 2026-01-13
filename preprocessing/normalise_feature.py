# preprocessing/normalise_feature.py
from .base import Preprocessor
from logs.logger import get_logger
import pandas as pd
import numpy as np
from typing import Optional


class NormaliseFeature(Preprocessor):
    """
    Normalise one numeric feature against another and optionally log-transform the result.

    Parameters (via constructor or factory kwargs):
      - numerator: str (column name to be numerator)
      - denominator: str (column name to be denominator)
      - denom_transform: str, optional transform to apply to denominator before division. Supported: 'log', 'log1p', 'sqrt', 'none' (default 'log1p')
      - smoothing: float, additive smoothing applied to denominator after transform to avoid division by zero (default 1e-9)
      - result_col: str, optional name for new column. If omitted, defaults to `{numerator}_per_{denominator}`
      - post_log: bool, whether to apply a log1p to the ratio result to reduce skew (default True)
      - suffix: optional suffix added if result_col omitted (default '_per_')

    Example usage:
      NormaliseFeature(numerator='num_Supported_Languages', denominator='num_Genres', denom_transform='log1p', post_log=True)
    Produces column: 'num_Supported_Languages_per_num_Genres' (or explicit `result_col`) with values: numerator / (log1p(denominator) + smoothing) and then log1p applied to the ratio if post_log=True.
    """

    def __init__(self, numerator: Optional[str] = None, denominator: Optional[str] = None,
                 denom_transform: str = 'log1p', smoothing: float = 1e-9, result_col: Optional[str] = None,
                 post_log: bool = True):
        self.numerator = numerator
        self.denominator = denominator
        self.denom_transform = denom_transform or 'log1p'
        self.smoothing = float(smoothing) if smoothing is not None else 1e-9
        self.result_col = result_col
        self.post_log = bool(post_log)
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info(f"Initialized NormaliseFeature(numerator={self.numerator}, denominator={self.denominator}, denom_transform={self.denom_transform}, smoothing={self.smoothing}, result_col={self.result_col}, post_log={self.post_log})")

    def fit(self, X):
        # stateless
        return self

    def _apply_denom_transform(self, denom_series: pd.Series) -> pd.Series:
        # Coerce to numeric
        ser = pd.to_numeric(denom_series, errors='coerce').astype(float)
        if self.denom_transform in ('log1p', 'log1'):
            return np.log1p(ser)
        if self.denom_transform == 'log':
            # if any non-positive values exist, shift to make positive
            minv = ser.min()
            if pd.isna(minv):
                return np.log(ser.replace({np.nan: 0}))
            if minv <= 0:
                shift = 1.0 - float(minv)
                self.logger.info(f"Shifting denominator by {shift} (min {minv}) for 'log' transform")
                return np.log(ser + shift)
            return np.log(ser)
        if self.denom_transform == 'sqrt':
            return np.sqrt(ser.clip(lower=0))
        # 'none' or unknown
        return ser

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(X, pd.DataFrame):
            self.logger.error("NormaliseFeature.transform expects a pandas DataFrame")
            raise ValueError("NormaliseFeature.transform expects a pandas DataFrame")

        if not self.numerator or not self.denominator:
            self.logger.error("NormaliseFeature requires 'numerator' and 'denominator' parameters")
            raise ValueError("NormaliseFeature requires 'numerator' and 'denominator' parameters")

        df = X.copy()
        num_col = self.numerator
        den_col = self.denominator

        if num_col not in df.columns:
            self.logger.warning(f"Numerator column '{num_col}' not found in DataFrame - creating result column with NaNs")
            num_ser = pd.Series([np.nan] * len(df), index=df.index)
        else:
            num_ser = pd.to_numeric(df[num_col], errors='coerce').astype(float)

        if den_col not in df.columns:
            self.logger.warning(f"Denominator column '{den_col}' not found in DataFrame - creating result column with NaNs")
            den_ser_trans = pd.Series([np.nan] * len(df), index=df.index)
        else:
            den_ser_trans = self._apply_denom_transform(df[den_col])

        # apply smoothing/add small epsilon to denominator to avoid division by zero
        den_safe = den_ser_trans + self.smoothing

        # compute ratio
        with np.errstate(divide='ignore', invalid='ignore'):
            ratio = num_ser / den_safe

        # optional post-log transform
        if self.post_log:
            # shift to ensure non-negative before log1p - log1p handles negative values gracefully but we prefer non-negative
            ratio = np.where(pd.isna(ratio), np.nan, np.log1p(np.maximum(ratio, 0)))
            ratio = pd.Series(ratio, index=df.index)
        else:
            ratio = pd.Series(ratio, index=df.index)

        out_col = self.result_col or f"{num_col}_per_{den_col}"
        # assign
        try:
            df.loc[:, out_col] = ratio
            self.logger.info(f"Created normalized feature column '{out_col}'")
        except Exception as e:
            self.logger.exception(f"Failed to assign normalized feature column '{out_col}': {e}")
            raise

        self.logger.info(f"DataFrame columns after NormaliseFeature: {df.columns.tolist()}")
        return df

    def fit_transform(self, X: pd.DataFrame) -> pd.DataFrame:
        return self.fit(X).transform(X)

    def get_params(self):
        return {"numerator": self.numerator, "denominator": self.denominator, "denom_transform": self.denom_transform, "smoothing": self.smoothing, "result_col": self.result_col, "post_log": self.post_log}

