#encoders/ordinal_encoder.py
from typing import Any, Iterable, List, Optional, Union, Dict
import numpy as np
import pandas as pd

from .base import Encoder


class OrdinalEncoder(Encoder):
    """
    Simple Ordinal encoder mapping categories to integers.

    Parameters:
    - categories: optional list of categories to enforce ordering; if None, learned from fit preserving order
    - dtype: optional dtype for output (e.g., int)
    - handle_unknown: 'error' or 'use_encoded_value' (default 'error')
    - unknown_value: int to use when handle_unknown='use_encoded_value' (default -1)
    """

    def __init__(self, categories: Optional[List[Any]] = None, dtype: Optional[type] = None,
                 handle_unknown: str = "error", unknown_value: int = -1):
        self._categories = list(categories) if categories is not None else None
        self.dtype = dtype
        self.handle_unknown = handle_unknown
        self.unknown_value = unknown_value
        self.categories_: List[Any] = []
        self.cat_to_int: Dict[Any, int] = {}
        self._fitted = False

    def fit(self, y: Iterable[Any]) -> "OrdinalEncoder":
        # learn categories order from input if not provided
        if self._categories is not None:
            seen = list(self._categories)
        else:
            seen = []
            for v in pd.Series(list(y)).dropna().astype(object).values.ravel():
                if v not in seen:
                    seen.append(v)
        self.categories_ = seen
        self.cat_to_int = {c: i for i, c in enumerate(self.categories_)}
        self._fitted = True
        return self

    def transform(self, y: Iterable[Any]) -> Union[pd.Series, np.ndarray]:
        if not self._fitted:
            raise ValueError("OrdinalEncoder has not been fitted yet. Call fit() first.")

        is_series = isinstance(y, pd.Series)
        if is_series:
            index = y.index
            name = y.name
            vals = y.values.ravel()
        else:
            vals = np.asarray(list(y)).ravel()
            index = None
            name = None

        out = np.full(len(vals), fill_value=self.unknown_value, dtype=self.dtype if self.dtype is not None else int)
        for i, v in enumerate(vals):
            if pd.isna(v):
                out[i] = self.unknown_value
                continue
            if v in self.cat_to_int:
                out[i] = self.cat_to_int[v]
            else:
                if self.handle_unknown == 'error':
                    raise ValueError(f"Unknown category '{v}' encountered in transform and handle_unknown='error'")
                else:
                    out[i] = self.unknown_value

        if is_series:
            ser = pd.Series(out, index=index, name=name)
            if self.dtype is not None:
                try:
                    ser = ser.astype(self.dtype)
                except Exception:
                    pass
            return ser
        return out

    def fit_transform(self, y: Iterable[Any]) -> Union[pd.Series, np.ndarray]:
        self.fit(y)
        return self.transform(y)

    def inverse_transform(self, y_enc: Iterable[Any]) -> Union[pd.Series, np.ndarray]:
        if not self._fitted:
            raise ValueError("OrdinalEncoder has not been fitted yet. Call fit() first.")

        is_series = isinstance(y_enc, pd.Series)
        if is_series:
            index = y_enc.index
            vals = y_enc.values.ravel()
            name = y_enc.name
        else:
            vals = np.asarray(list(y_enc)).ravel()
            index = None
            name = None

        int_to_cat = {i: c for c, i in self.cat_to_int.items()} if self.cat_to_int else {}
        out = []
        for v in vals:
            if pd.isna(v):
                out.append(None)
                continue
            try:
                iv = int(v)
            except Exception:
                out.append(None)
                continue
            if iv in int_to_cat:
                out.append(int_to_cat[iv])
            else:
                out.append(None)

        if is_series:
            return pd.Series(out, index=index, name=name)
        return np.array(out, dtype=object)


# alias
Ordinal = OrdinalEncoder

