# preprocessing/lowercase.py
from typing import Iterable, List, Optional
from .base import Preprocessor
from logs.logger import get_logger
import pandas as pd


class Lowercase(Preprocessor):
    """Simple preprocessor that lowercases text.

    Behaviour:
    - Can operate on iterables / pandas Series (text preprocessing) OR on a
      pandas.DataFrame when `fields` / `field` is provided (column-wise).
    - Returns a list of lowercased strings for text input, or a DataFrame when
      operating on columns.
    """

    def __init__(self, fields: Optional[List[str]] = None, field: Optional[str] = None):
        """Initialise Lowercase.

        Parameters
        - fields: optional list of column names to lowercase when transform is
          passed a pandas.DataFrame.
        - field: legacy single-field name (will be converted to `fields`).
        """
        self.logger = get_logger(self.__class__.__name__)
        # normalize legacy `field` -> `fields`
        if field is not None and (fields is None or len(fields) == 0):
            fields = [field]
        self.fields = fields or []
        self.lower_case = True
        self.logger.info(f"Initialized Lowercase preprocessor fields={self.fields}")

    def fit(self, X: Iterable[str]):
        # stateless
        return self

    def transform(self, X: Iterable[str]):
        """Transform input.

        If a pandas.DataFrame is provided AND `fields` is set, lowercases each
        column in-place on a copy and returns the DataFrame. Otherwise behaves
        as a text preprocessor and returns a list of lowercased strings.
        """
        self.logger.info("Starting Lowercase transformation")

        # pandas DataFrame path when fields are provided
        if isinstance(X, pd.DataFrame):
            if not self.fields:
                # nothing to do
                self.logger.warning("Lowercase.transform called with DataFrame but no `fields` configured; returning original DataFrame")
                return X
            df = X.copy()
            for col in self.fields:
                if col not in df.columns:
                    self.logger.info(f"Column '{col}' not found in DataFrame; skipping")
                    continue
                try:
                    # preserve NaNs; operate on non-null values
                    df[col] = df[col].apply(lambda v: ("" if v is None else str(v)).lower() if pd.notna(v) else v)
                except Exception as e:
                    self.logger.warning(f"Failed to lowercase column '{col}': {e}; leaving original values")
            self.logger.info("Completed Lowercase transformation on DataFrame")
            return df

        # iterable / Series path: operate element-wise and return list
        out: List[str] = []
        for i, v in enumerate(X):
            try:
                s = "" if v is None else str(v)
                out.append(s.lower())
            except Exception as e:
                self.logger.warning(f"Lowercase transform failed for index {i}: {e}; using original value")
                out.append(str(v))
        self.logger.info("Completed Lowercase transformation")
        return out

    def get_params(self) -> dict:
        return {"fields": self.fields, "lower_case": self.lower_case}
