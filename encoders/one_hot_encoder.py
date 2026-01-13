#encoders/one_hot_encoder.py
from typing import Any, Iterable, List, Optional, Union
import numpy as np
import pandas as pd

from .base import Encoder


class OneHotEncoder(Encoder):
    """
    Simple one-hot encoder that converts a sequence of single categorical labels
    into a binary matrix (or DataFrame).

    Parameters:
    - dtype: optional numpy/pandas dtype for output (e.g., int, float)
    - handle_unknown: 'ignore' or 'error' when transforming unseen categories
    """

    def __init__(self, dtype: Optional[type] = None, handle_unknown: str = "ignore"):
        self.dtype = dtype
        self.handle_unknown = handle_unknown
        self.categories_: List[Any] = []
        self._fitted = False

    def fit(self, y: Iterable[Any]) -> "OneHotEncoder":
        # Accept pandas Series, numpy array or other iterable; flatten to 1d
        if isinstance(y, pd.Series):
            arr = y.dropna().astype(object).values.ravel()
        else:
            arr = np.asarray(list(y)).ravel()
        # Determine unique categories preserving order
        seen = []
        for v in arr:
            if v not in seen:
                seen.append(v)
        self.categories_ = seen
        self._fitted = True
        return self

    def transform(self, y: Iterable[Any]) -> Union[pd.DataFrame, np.ndarray]:
        if not self._fitted:
            raise ValueError("OneHotEncoder has not been fitted yet. Call fit() first.")

        # Prepare input iterable
        if isinstance(y, pd.Series):
            index = y.index
            name = y.name or "feature"
            values = y.values.ravel()
        else:
            values = np.asarray(list(y)).ravel()
            index = None
            name = None

        n = len(values)
        m = len(self.categories_)
        arr = np.zeros((n, m), dtype=self.dtype if self.dtype is not None else int)

        cat_to_idx = {c: i for i, c in enumerate(self.categories_)}

        for i, v in enumerate(values):
            if pd.isna(v):
                continue
            if v not in cat_to_idx:
                if self.handle_unknown == "error":
                    raise ValueError(f"Unknown category '{v}' encountered in transform and handle_unknown='error'")
                else:
                    # ignore unknown category
                    continue
            arr[i, cat_to_idx[v]] = 1

        if index is not None:
            colnames = [f"{name}__{c}" for c in self.categories_]
            df = pd.DataFrame(arr, index=index, columns=colnames)
            if self.dtype is not None:
                df = df.astype(self.dtype)
            return df
        return arr

    def fit_transform(self, y: Iterable[Any]) -> Union[pd.DataFrame, np.ndarray]:
        self.fit(y)
        return self.transform(y)

    def inverse_transform(self, y_enc: Iterable[Any]) -> Union[pd.Series, List[Any], np.ndarray]:
        if not self._fitted:
            raise ValueError("OneHotEncoder has not been fitted yet. Call fit() first.")

        # If DataFrame provided, use columns order to map back
        if isinstance(y_enc, pd.DataFrame):
            arr = y_enc.values
            idx = y_enc.index
            colnames = list(y_enc.columns)
        else:
            arr = np.asarray(y_enc)
            idx = None
            colnames = None

        # If arr is 2D
        if arr.ndim == 2:
            out = []
            for row in arr:
                # find indices with highest value (presence)
                ones = np.where(row != 0)[0]
                if len(ones) == 0:
                    out.append(None)
                else:
                    # if multiple, return first in categories_ order
                    out.append(self.categories_[int(ones[0])])
            if idx is not None:
                return pd.Series(out, index=idx)
            return np.array(out, dtype=object)
        else:
            # 1D array of labels (unlikely) -> map indices to categories
            return np.array([self.categories_[int(i)] if not pd.isna(i) else None for i in arr], dtype=object)


# alias
OneHot = OneHotEncoder

