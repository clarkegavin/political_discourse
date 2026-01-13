# preprocessing/cyclic_encode.py
from .base import Preprocessor
from logs.logger import get_logger
import pandas as pd
import numpy as np
from typing import List, Optional, Union, Dict, Any


class CyclicEncode(Preprocessor):
    """
    Encode numeric cyclical/temporal features into sine and cosine components.

    Parameters:
      - columns: list[str] or str, columns to encode
      - period: int or dict mapping column->period (default 12)
      - drop_original: bool (default True)

    Example:
      CyclicEncode(columns=['release_month'], period=12, drop_original=True)

    The transform will create for each column `col` two new columns:
      - `{col}_sin` with np.sin(2*pi*col / P)
      - `{col}_cos` with np.cos(2*pi*col / P)

    Missing values are preserved as NaN in the new columns.
    """

    def __init__(self, columns: Optional[Union[List[str], str]] = None, period: Union[int, Dict[str, int]] = 12, drop_original: bool = True):
        self.columns = [columns] if isinstance(columns, str) else (list(columns) if columns is not None else [])
        self.period = period
        self.drop_original = bool(drop_original)
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info(f"Initialized CyclicEncode(columns={self.columns}, period={self.period}, drop_original={self.drop_original})")

    def fit(self, X):
        # Stateless transformer
        return self

    def _get_period_for(self, col: str) -> int:
        if isinstance(self.period, dict):
            return int(self.period.get(col, 12))
        try:
            return int(self.period)
        except Exception:
            return 12

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(X, pd.DataFrame):
            self.logger.error("CyclicEncode.transform expects a pandas DataFrame")
            raise ValueError("CyclicEncode.transform expects a pandas DataFrame")

        df = X.copy()
        cols = self.columns or []
        if not cols:
            self.logger.warning("CyclicEncode called with no columns configured; nothing to do")
            return df

        for col in cols:
            if col not in df.columns:
                self.logger.warning(f"CyclicEncode: column '{col}' not found in DataFrame - skipping")
                continue

            # coerce to numeric but preserve NaNs
            ser = pd.to_numeric(df[col], errors='coerce')
            P = self._get_period_for(col)
            if P == 0:
                self.logger.warning(f"CyclicEncode: period for column '{col}' is 0 - skipping")
                continue

            factor = 2 * np.pi / float(P)
            sin_col = f"{col}_sin"
            cos_col = f"{col}_cos"

            # compute while preserving NaNs
            with np.errstate(invalid='ignore'):
                sin_vals = np.sin(ser * factor)
                cos_vals = np.cos(ser * factor)

            df.loc[:, sin_col] = sin_vals
            df.loc[:, cos_col] = cos_vals
            self.logger.info(f"CyclicEncode: created columns '{sin_col}', '{cos_col}' from '{col}' with period={P}")

            if self.drop_original:
                try:
                    df = df.drop(columns=[col])
                    self.logger.info(f"CyclicEncode: dropped original column '{col}'")
                except Exception as e:
                    self.logger.warning(f"CyclicEncode: failed to drop original column '{col}': {e}")

        return df

    def fit_transform(self, X: pd.DataFrame) -> pd.DataFrame:
        return self.fit(X).transform(X)

    def get_params(self) -> Dict[str, Any]:
        return {'columns': self.columns, 'period': self.period, 'drop_original': self.drop_original}

