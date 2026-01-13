# preprocessing/catalog_count.py
from .base import Preprocessor
from logs.logger import get_logger
import pandas as pd
from typing import List, Optional, Any
from collections import Counter


class CatalogCount(Preprocessor):
    """Count how many rows each atomic value appears in, then aggregate per-row.

    Parameters
    - columns: list of columns to process (if omitted, all columns)
    - sep: separator for splitting string cells (default ',')
    - prefix: prefix to use for new column names (default 'catalog_count_')
    - drop_original: whether to drop the original column after creating counts (default False)
    - agg: how to aggregate counts for multi-valued cells: 'sum' (default), 'max', 'mean', 'min'

    Behavior
    - Treats each row as containing a set of items for the column (duplicate items in same cell counted once).
    - Builds a global Counter mapping item -> number of rows where it appears.
    - For each row, computes an aggregated value from the counts of the items present according to `agg`.
    """

    def __init__(self, columns: Optional[List[str]] = None, sep: str = ',', prefix: str = 'catalog_count_', drop_original: bool = False, agg: str = 'sum'):
        self.columns = columns or []
        self.sep = sep
        self.prefix = prefix or 'catalog_count_'
        self.drop_original = bool(drop_original)
        self.agg = agg if agg in ('sum', 'max', 'mean', 'min') else 'sum'
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info(f"Initialized CatalogCount(columns={self.columns}, sep='{self.sep}', prefix='{self.prefix}', drop_original={self.drop_original}, agg={self.agg})")

    def fit(self, X: Any):
        # Stateless for now; counts computed during transform since they depend on the DataFrame passed
        return self

    def _parse_cell(self, val):
        # Return set of cleaned items present in the cell
        if pd.isna(val):
            return set()
        if isinstance(val, (list, tuple, set)):
            items = [str(v).strip() for v in val if v is not None and str(v).strip() != '']
            return set(items)
        # otherwise string
        try:
            s = str(val)
        except Exception:
            return set()
        items = [p.strip() for p in s.split(self.sep) if p.strip() != '']
        return set(items)

    def transform(self, X: Any) -> pd.DataFrame:
        if not isinstance(X, pd.DataFrame):
            self.logger.error("CatalogCount.transform expects a pandas DataFrame")
            raise ValueError("CatalogCount.transform expects a pandas DataFrame")

        df = X.copy()
        cols = self.columns or list(df.columns)

        for c in cols:
            if c not in df.columns:
                self.logger.warning(f"Column '{c}' not found in DataFrame - skipping")
                continue

            # Build counter of item -> number of rows where it appears
            row_sets = df[c].apply(self._parse_cell)
            counter = Counter()
            for s in row_sets:
                # count presence once per row
                for item in s:
                    counter[item] += 1

            # compute aggregated value per row
            def _agg_val(s):
                if not s:
                    return 0 if self.agg in ('sum', 'max', 'min') else 0.0
                counts = [counter.get(item, 0) for item in s]
                if self.agg == 'sum':
                    return sum(counts)
                elif self.agg == 'max':
                    return max(counts)
                elif self.agg == 'min':
                    return min(counts)
                elif self.agg == 'mean':
                    return float(sum(counts)) / len(counts)
                return sum(counts)

            new_col = f"{self.prefix}{c}"
            self.logger.info(f"Generating catalog counts '{new_col}' for column '{c}' with agg='{self.agg}'")
            try:
                df.loc[:, new_col] = row_sets.apply(_agg_val)
            except Exception as e:
                self.logger.exception(f"Failed to create catalog count column for '{c}': {e}")
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
        return {'columns': self.columns, 'sep': self.sep, 'prefix': self.prefix, 'drop_original': self.drop_original, 'agg': self.agg}

