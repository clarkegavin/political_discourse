#encoders/label_encoder.py
from typing import Any, Iterable, Optional, Union
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder as _SkLabelEncoder

from .base import Encoder


class SklearnLabelEncoder(Encoder):
    """A thin wrapper around sklearn.preprocessing.LabelEncoder.

    Accepts pandas.Series, numpy arrays, or other iterables. Returns the same
    type where reasonable (Series -> Series, list/ndarray -> ndarray).
    """

    def __init__(self, dtype: Optional[type] = None):
        self._enc = _SkLabelEncoder()
        self._fitted = False
        self._dtype = dtype

    def fit(self, y: Iterable[Any]) -> "SklearnLabelEncoder":
        arr = self._to_numpy(y)
        self._enc.fit(arr)
        self._fitted = True
        return self

    def transform(self, y: Iterable[Any]) -> Union[pd.Series, np.ndarray]:
        if not self._fitted:
            raise ValueError("Encoder has not been fitted yet. Call fit() first.")
        arr = self._to_numpy(y)
        transformed = self._enc.transform(arr)
        if isinstance(y, pd.Series):
            result = pd.Series(transformed, index=y.index, name=y.name)
            if self._dtype:
                result = result.astype(self._dtype)
            return result
        return transformed

    def fit_transform(self, y: Iterable[Any]) -> Union[pd.Series, np.ndarray]:
        self.fit(y)
        return self.transform(y)

    def inverse_transform(self, y_enc: Iterable[Any]) -> Union[pd.Series, np.ndarray]:
        if not self._fitted:
            raise ValueError("Encoder has not been fitted yet. Call fit() first.")
        # sklearn expects 1d array-like
        arr = np.asarray(y_enc)
        inv = self._enc.inverse_transform(arr)
        # If input was pandas Series return Series
        if isinstance(y_enc, pd.Series):
            return pd.Series(inv, index=y_enc.index, name=y_enc.name)
        return inv

    def _to_numpy(self, y: Iterable[Any]) -> np.ndarray:
        if isinstance(y, pd.Series):
            return y.values.ravel()
        if isinstance(y, np.ndarray):
            return y.ravel()
        return np.asarray(list(y))


# provide a short alias for external imports
LabelEncoder = SklearnLabelEncoder

