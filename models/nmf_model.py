from sklearn.decomposition import NMF
from typing import Dict, Any
from .base import Model

class NMFModel(Model):
    def __init__(self, name: str = "nmf", **params: Dict[str, Any]):
        super().__init__(name, **params)
        self.logger = get_logger(f"NMFModel.{name}")
        self.params = params or {}
        self.model = None

    def build(self):
        self.model = NMF(**self.params)
        return self.model

    def fit(self, X, y=None):
        if self.model is None:
            self.build()
        self.model.fit(X)
        return self

    def transform(self, X):
        if self.model is None:
            raise RuntimeError("Model not built/fitted")
        return self.model.transform(X)

    def fit_transform(self, X):
        if self.model is None:
            self.build()
        self.model.fit(X)
        return self.model.transform(X)
# models/nmf_model.py
from .base import Model
from logs.logger import get_logger
