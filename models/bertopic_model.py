# models/bertopic_model.py
from .base import Model
from logs.logger import get_logger
from typing import Any, Dict
from bertopic import BERTopic


class BERTopicModel(Model):
    """Wrapper around BERTopic to fit into ModelFactory pattern."""
    def __init__(self, name: str = "bertopic", **params: Dict[str, Any]):
        super().__init__(name, **params)
        self.logger = get_logger(self.__class__.__name__)
        self.params = params or {}
        self.model = None
        self.topics_ = None
        self.probs_ = None

    def build(self):
        self.logger.info(f"Building BERTopicModel with parameters: {self.params}")
        self.model = BERTopic(**self.params)
        return self.model

    def fit(self, X, y=None):
        self.logger.info("Fitting BERTopicModel")
        if self.model is None:
            self.build()
        # X is expected to be list-like of documents
        self.topics_, self.probs_ = self.model.fit_transform(X)
        return self

    def transform(self, X):
        self.logger.info("Transforming data with BERTopicModel")
        if self.model is None:
            self.logger.error("Attempted to transform with unbuilt/unfitted model")
            raise RuntimeError("Model not built/fitted")
        return self.model.transform(X)

    def fit_transform(self, X):
        self.logger.info("Starting fit_transform for BERTopicModel")
        if self.model is None:
            self.build()
        return self.model.fit_transform(X)

    def get_topic_info(self):
        if self.model is None:
            self.logger.error("Attempted to get topic info from unbuilt/unfitted model")
        return self.model.get_topic_info()

    def get_topic(self, topic_id):
        if self.model is None:
            self.logger.error("Attempted to get topic from unbuilt/unfitted model")
        return self.model.get_topic(topic_id)

    def get_topics(self):
        if self.model is None:
            self.logger.error("Attempted to get topics from unbuilt/unfitted model")
        return self.model.get_topics()

