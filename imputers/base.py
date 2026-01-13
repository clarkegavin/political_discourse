# imputers/base.py
from abc import ABC, abstractmethod
from typing import Any, Iterable, List


class Imputer(ABC):
    """Abstract imputer interface similar to sklearn transformers.

    Implementations should provide fit/transform and fit_transform.
    """

    @abstractmethod
    def fit(self, X):
        """Fit imputer to data X if required. Return self."""

    @abstractmethod
    def transform(self, X):
        """Transform X and return imputed result (DataFrame or iterable)."""

    def fit_transform(self, X):
        self.fit(X)
        return self.transform(X)

