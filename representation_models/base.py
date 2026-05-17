from abc import ABC, abstractmethod

class RepresentationModel(ABC):
    """Abstract base class for representation models."""

    @abstractmethod
    def fit(self, X):
        """Fit the model to the data."""
        raise NotImplementedError

    @abstractmethod
    def transform(self, X):
        """Transform the data using the fitted model."""
        raise NotImplementedError

    def fit_transform(self, X):
        """Fit the model and transform the data."""
        self.fit(X)
        return self.transform(X)