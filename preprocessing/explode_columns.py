# preprocessing/explode_columns.py
from .base import Preprocessor
from logs.logger import get_logger
import pandas as pd
from typing import List, Optional

class ExplodeColumns(Preprocessor):
    """
    ExplodeColumns preprocessor

    Splits comma (or custom-separator) separated values in specified columns and
    explodes them into multiple rows (one value per row), preserving other columns.

    Parameters
    - columns: List[str] - column names to explode (silently ignored if missing)
    - sep: str - separator used to split the cell values (default: ',')
    - strip: bool - whether to strip whitespace from split tokens (default: True)
    - drop_empty: bool - whether to drop empty tokens after splitting (default: True)
    - reset_index: bool - whether to reset the returned DataFrame index (default: True)
    """

    def __init__(self, columns: Optional[List[str]] = None, sep: str = ',', strip: bool = True, drop_empty: bool = True, reset_index: bool = True):
        self.columns = columns or []
        self.sep = sep
        self.strip = strip
        self.drop_empty = drop_empty
        self.reset_index = reset_index
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info(f"Initialized ExplodeColumns with columns={self.columns}, sep='{self.sep}', strip={self.strip}, drop_empty={self.drop_empty}, reset_index={self.reset_index}")

    def fit(self, X):
        # stateless
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transform the DataFrame by exploding configured columns.

        Behaviour:
        - For each column in self.columns that exists in X, convert values to string
          (preserving NaN) and split by `sep`, producing lists of tokens. Then explode the lists.
        - If drop_empty is True, remove empty tokens (after optional strip).
        - If reset_index is True, reset the index on the returned DataFrame.

        Returns a transformed DataFrame.
        """
        self.logger.info("Starting ExplodeColumns.transform")
        if not isinstance(X, pd.DataFrame):
            self.logger.error("ExplodeColumns.transform expects a pandas DataFrame")
            raise ValueError("ExplodeColumns.transform expects a pandas DataFrame")

        df = X.copy()

        for col in self.columns:
            if col not in df.columns:
                self.logger.info(f"Column '{col}' not found in DataFrame, skipping")
                continue

            self.logger.info(f"Exploding column: {col}")

            # Create a Series of lists by splitting strings; preserve NaN values
            ser = df[col]

            def _to_list(v):
                if pd.isna(v):
                    return [pd.NA]
                # If already a list/tuple, convert elements to str
                if isinstance(v, (list, tuple, set)):
                    items = list(v)
                else:
                    # Coerce to string and split
                    items = str(v).split(self.sep)
                processed = []
                for it in items:
                    it_str = str(it)
                    if self.strip:
                        it_str = it_str.strip()
                    if self.drop_empty and (it_str == '' or it_str.lower() == 'nan'):
                        continue
                    processed.append(it_str)
                # If after processing it's empty, keep a single NA so that explode keeps the row
                if len(processed) == 0:
                    return [pd.NA]
                return processed

            try:
                list_ser = ser.map(_to_list)
            except Exception as e:
                self.logger.exception(f"Failed to map column '{col}' to list: {e}")
                # fallback: try simple splitting with fillna
                list_ser = ser.fillna('').astype(str).map(lambda v: [s.strip() for s in v.split(self.sep) if s.strip()!=''])

            df[col] = list_ser
            # explode the column
            df = df.explode(col)

            # Optionally convert pandas.NA back to NaN for consistency
            try:
                df[col] = df[col].where(pd.notna(df[col]), None)
            except Exception:
                pass

        if self.reset_index:
            df = df.reset_index(drop=True)

        self.logger.info("Completed ExplodeColumns.transform")
        return df

    def get_params(self) -> dict:
        return {"columns": self.columns, "sep": self.sep, "strip": self.strip, "drop_empty": self.drop_empty, "reset_index": self.reset_index}

