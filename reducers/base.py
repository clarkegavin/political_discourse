# reducers/base.py
from abc import ABC, abstractmethod
from typing import Any


class Reducer(ABC):
    """Abstract base class for dimensionality reducers."""

    @abstractmethod
    def fit(self, X: Any):
        raise NotImplementedError

    @abstractmethod
    def transform(self, X: Any):
        raise NotImplementedError

    def fit_transform(self, X: Any):
        self.fit(X)
        return self.transform(X)

