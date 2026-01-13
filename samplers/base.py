#samples/base.py
from abc import ABC, abstractmethod

class Sampler(ABC):
    """Abstract sampler interface for sampling data points.

    Methods:
    - sample: sample data points according to some strategy
    """

    @abstractmethod
    def fit_resample(self, X, y):
        """Fit the sampler and resample the data.

        Args:
            X: Features to sample from.
            y: Labels corresponding to X.
        Returns:
            Resampled features and labels.
        """
        pass





