# models/bertopic_model.py
from .base import Model
from logs.logger import get_logger
import importlib
from typing import Any, Dict
from bertopic import BERTopic


class BERTopicModel(Model):
    """Wrapper around BERTopic to fit into ModelFactory pattern."""
    def __init__(self, name: str = "bertopic", **params: Dict[str, Any]):
        super().__init__(name, **params)
        self.logger = get_logger(f"BERTopicModel.{name}")
        self.params = params or {}
        self.model = None
        self.topics_ = None
        self.probs_ = None

    def build(self):
        self.model = BERTopic(**self.params)
        return self.model

    def fit(self, X, y=None):
        if self.model is None:
            self.build()
        # X is expected to be list-like of documents
        self.topics_, self.probs_ = self.model.fit_transform(X)
        return self

    def transform(self, X):
        if self.model is None:
            self.logger.error("Attempted to transform with unbuilt/unfitted model")
            raise RuntimeError("Model not built/fitted")
        return self.model.transform(X)

    def fit_transform(self, X):
        if self.model is None:
            self.build()
        return self.model.fit_transform(X)

