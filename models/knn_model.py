from .base import Model
from logs.logger import get_logger
from sklearn.neighbors import KNeighborsClassifier
from typing import Dict, Any

class KNNClassificationModel(Model):
    """
    K-Nearest Neighbors Classification Model.
    """

    def __init__(self, name: str=None, **params):
        self.logger = get_logger(f"KNNClassificationModel.{name}")
        self.logger.info(f"Initializing KNN model with name {name} and params: {params}")
        super().__init__(name, **params)
        self.model = None
        self.params = params

    def build(self):
        self.model = KNeighborsClassifier(**self.params)
        self.logger.info(f"Built KNN model with params: {self.params}")
        return self
