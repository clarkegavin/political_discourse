# models/lda_model.py
from .base import Model
from logs.logger import get_logger
from sklearn.decomposition import LatentDirichletAllocation
from typing import Dict, Any

class LDAModel(Model):
    def __init__(self, name: str = "lda", **params: Dict[str, Any]):
        super().__init__(name, **params)
        self.logger = get_logger(f"LDAModel.{name}")
        self.params = params or {}
        self.model = None

    def build(self):
        self.model = LatentDirichletAllocation(**self.params)
        return self.model

    def fit(self, X, y=None):
        # X expected to be document-term matrix
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

