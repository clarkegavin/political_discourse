#scalers/standard_scaler.py
from .base import Scaler
from sklearn.preprocessing import StandardScaler
from logs.logger import get_logger

class StandardDataScaler(Scaler):
    """Standardizes features by removing the mean and scaling to unit variance."""

    def __init__(self, **kwargs):
        self.logger = get_logger("StandardDataScaler")
        self.logger.info(f"Initializing StandardDataScaler with params: {kwargs}")
        self.scaler = StandardScaler(**kwargs)
        self.logger.info("StandardDataScaler initialized.")



    def fit(self, X):
        self.scaler.fit(X)

    def transform(self, X):
        return self.scaler.transform(X)

    def fit_transform(self, X):
        return self.scaler.fit_transform(X)

    def execute(self, X):
        """Convenience method to fit and transform in one step."""
        self.logger.info("Executing fit_transform on data.")

        return self.fit_transform(X)