#encoders/multi_hot_encoder.py
from typing import Any, Iterable, List, Optional, Union
import numpy as np
import pandas as pd
from scipy.sparse import lil_matrix
from .base import Encoder
from logs.logger import get_logger


class MultiHotEncoder(Encoder):
    """
    Encoder for multi-label fields where each cell may contain multiple categories
    (e.g., comma-separated platforms). Produces a binary indicator matrix with one
    column per known category and a 1 where the category is present in the cell.

    Parameters:
    - sep: separator used when transforming raw string cells (default: ',')
    - dtype: output dtype (default int)
    - handle_unknown: 'ignore' or 'error' when new categories are seen in transform
    """

    def __init__(self, sep: str = ',', dtype: Optional[type] = None, handle_unknown: str = 'ignore'):
        self.sep = sep
        self.dtype = dtype
        self.handle_unknown = handle_unknown
        self.categories_: List[str] = []
        self._fitted = False
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info(f"Initialized MultiHotEncoder with sep='{self.sep}', dtype={self.dtype}, handle_unknown='{self.handle_unknown}'")

    def _split_cell(self, cell):
        if pd.isna(cell):
            return []
        if isinstance(cell, (list, tuple, set)):
            return [str(x).strip() for x in cell if x is not None and str(x).strip() != '']
        s = str(cell)
        # split and strip
        parts = [p.strip() for p in s.split(self.sep) if p.strip() != '']
        return parts

    def fit(self, y: Iterable[Any]) -> "MultiHotEncoder":
        # collect unique category tokens across all cells, preserving order
        seen = []
        for cell in y:
            tokens = self._split_cell(cell)
            for t in tokens:
                if t not in seen:
                    seen.append(t)
        self.categories_ = seen
        self._fitted = True
        return self

    # def transform(self, y: Iterable[Any]) -> Union[pd.DataFrame, np.ndarray]:
    #     if not self._fitted:
    #         raise ValueError("MultiHotEncoder has not been fitted yet. Call fit() first.")
    #
    #     # Accept Series for index preservation
    #     index = None
    #     name = None
    #     if isinstance(y, pd.Series):
    #         index = y.index
    #         name = y.name or 'feature'
    #
    #     # Prepare output array
    #     values = list(y)
    #     n = len(values)
    #     m = len(self.categories_)
    #     arr = np.zeros((n, m), dtype=self.dtype if self.dtype is not None else int)
    #     cat_to_idx = {c: i for i, c in enumerate(self.categories_)}
    #
    #     for i, cell in enumerate(values):
    #         tokens = self._split_cell(cell)
    #         for t in tokens:
    #             if t not in cat_to_idx:
    #                 if self.handle_unknown == 'error':
    #                     raise ValueError(f"Unknown token '{t}' encountered in transform and handle_unknown='error'")
    #                 else:
    #                     continue
    #             arr[i, cat_to_idx[t]] = 1
    #
    #     if index is not None:
    #         colnames = [f"{name}__{c}" for c in self.categories_]
    #         df = pd.DataFrame(arr, index=index, columns=colnames)
    #         if self.dtype is not None:
    #             df = df.astype(self.dtype)
    #         return df
    #     return arr

    def transform(self, y: Iterable[Any]) -> pd.DataFrame:
        if not self._fitted:
            raise ValueError("MultiHotEncoder has not been fitted yet.")

        if not isinstance(y, pd.Series):
            y = pd.Series(y)

        n = len(y)
        m = len(self.categories_)
        name = y.name or "feature"

        cat_to_idx = {c: i for i, c in enumerate(self.categories_)}

        # sparse matrix (LIL is efficient for incremental writes)
        mat = lil_matrix((n, m), dtype=self.dtype)

        for i, cell in enumerate(y):
            for t in self._split_cell(cell):
                idx = cat_to_idx.get(t)
                if idx is None:
                    if self.handle_unknown == "error":
                        raise ValueError(f"Unknown token '{t}'")
                    continue
                mat[i, idx] = 1

        # convert to CSR for efficiency
        mat = mat.tocsr()

        colnames = [f"{name}__{c}" for c in self.categories_]

        #pandas sparse DataFrame
        df_sparse = pd.DataFrame.sparse.from_spmatrix(
            mat,
            index=y.index,
            columns=colnames
        )

        self.logger.info(f"MultiHotEncoder transformed input into sparse DataFrame with shape {df_sparse.shape}")
        self.logger.info(f"MultiHotEncoder memory usage reduced bytes to {df_sparse.memory_usage(deep=True).sum()} bytes")

        return df_sparse

    def fit_transform(self, y: Iterable[Any]) -> Union[pd.DataFrame, np.ndarray]:
        self.fit(y)
        return self.transform(y)

    def inverse_transform(self, y_enc: Iterable[Any]) -> Union[List[List[str]], pd.Series]:
        if not self._fitted:
            raise ValueError("MultiHotEncoder has not been fitted yet. Call fit() first.")

        if isinstance(y_enc, pd.DataFrame):
            arr = y_enc.values
            idx = y_enc.index
        else:
            arr = np.asarray(y_enc)
            idx = None

        out = []
        for row in arr:
            ones = np.where(row != 0)[0]
            tokens = [self.categories_[int(i)] for i in ones]
            out.append(tokens)

        if idx is not None:
            return pd.Series(out, index=idx)
        return out

# alias
MultiHot = MultiHotEncoder

