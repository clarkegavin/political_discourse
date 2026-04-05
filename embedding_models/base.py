from abc import ABC, abstractmethod

class EmbeddingModel(ABC):
    """Abstract embedding model interface."""

    @abstractmethod
    def fit(self, X):
        """Fit the embedding model to the data."""
        pass

    @abstractmethod
    def transform(self, X):
        """Transform the data to the embedding space."""
        pass

    @abstractmethod
    def get_feature_names(self):
        pass

    def fit_transform(self, X):
        """Convenience method: fit and transform in one step."""
        self.fit(X)
        return self.transform(X)




