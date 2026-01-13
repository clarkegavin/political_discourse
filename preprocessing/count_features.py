# preprocessing/count_features.py
from .base import Preprocessor
from logs.logger import get_logger
import pandas as pd
from typing import List, Optional, Any


class CountFeatures(Preprocessor):
    """Generate count-based features for columns containing list-like strings.

    Example:
      columns: ['Publishers']
      sep: ','
      drop_original: False
      prefix: 'num_'

    For a cell 'A,B' -> count 2. Empty or NaN -> 0.
    """

    def __init__(self, columns: Optional[List[str]] = None, sep: str = ',', drop_original: bool = False, prefix: str = 'num_'):
        self.columns = columns or []
        self.sep = sep
        self.drop_original = bool(drop_original)
        self.prefix = prefix or 'num_'
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info(f"Initialized CountFeatures(columns={self.columns}, sep='{self.sep}', drop_original={self.drop_original}, prefix='{self.prefix}')")

    def fit(self, X: Any):
        # Stateless
        return self

    def _count_cell(self, val):
        if pd.isna(val):
            return 0
        # If it's already a list/tuple, count non-empty items
        if isinstance(val, (list, tuple)):
            return sum(1 for v in val if v is not None and str(v).strip() != '')
        # Otherwise treat as string and split
        try:
            s = str(val)
        except Exception:
            return 0
        parts = [p.strip() for p in s.split(self.sep) if p.strip() != '']
        return len(parts)

    def transform(self, X: Any) -> pd.DataFrame:
        if not isinstance(X, pd.DataFrame):
            self.logger.error("CountFeatures.transform expects a pandas DataFrame")
            raise ValueError("CountFeatures.transform expects a pandas DataFrame")

        df = X.copy()
        cols = self.columns or list(df.columns)
        for c in cols:
            if c not in df.columns:
                self.logger.warning(f"Column '{c}' not found in DataFrame - skipping")
                continue

            new_col = f"{self.prefix}{c}"
            self.logger.info(f"Generating count feature '{new_col}' from column '{c}' (sep='{self.sep}')")
            try:
                df.loc[:, new_col] = df[c].apply(self._count_cell)
            except Exception as e:
                self.logger.exception(f"Failed to create count column for '{c}': {e}")
                continue

            if self.drop_original:
                try:
                    df = df.drop(columns=[c])
                except Exception as e:
                    self.logger.warning(f"Failed to drop original column '{c}': {e}")

        return df

    def fit_transform(self, X: Any) -> pd.DataFrame:
        return self.fit(X).transform(X)

    def get_params(self):
        return {'columns': self.columns, 'sep': self.sep, 'drop_original': self.drop_original, 'prefix': self.prefix}

