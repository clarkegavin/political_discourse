# preprocessing/base.py
from abc import ABC, abstractmethod
from typing import Iterable, List


class Preprocessor(ABC):
    """Abstract preprocessor interface similar to sklearn transformers.

    Methods:
    - fit: optional, used when the preprocessor needs to learn state from data
    - transform: apply transformation
    - fit_transform: convenience
    """

    @abstractmethod
    def fit(self, X: Iterable[str]):
        """Learn any state from X if required. Return self."""

    @abstractmethod
    def transform(self, X: Iterable[str]) -> List[str]:
        """Transform input iterable of strings and return transformed list."""

    def fit_transform(self, X: Iterable[str]) -> List[str]:
        self.fit(X)
        return self.transform(X)

