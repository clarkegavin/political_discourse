#scalers/base.py
from abc import ABC, abstractmethod

class Scaler(ABC):
    """Abstract base class for data scalers."""

    @abstractmethod
    def fit(self, X):
        raise NotImplementedError

    @abstractmethod
    def transform(self, X):
        raise NotImplementedError

    def fit_transform(self, X):
        self.fit(X)
        return self.transform(X)