#scalers/robust_scaler.py
from .base import Scaler
from sklearn.preprocessing import RobustScaler

class RobustDataScaler(Scaler):
    """Scales features using statistics that are robust to outliers."""

    def __init__(self):
        self.scaler = RobustScaler()

    def fit(self, X):
        self.scaler.fit(X)

    def transform(self, X):
        return self.scaler.transform(X)

    def fit_transform(self, X):
        return self.scaler.fit_transform(X)