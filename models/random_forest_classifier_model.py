from sklearn.ensemble import RandomForestClassifier
from logs.logger import get_logger
from .base import Model

class RandomForestClassifierModel(Model):
    """
    Random Forest Classifier Model.
    """

    def __init__(self, name: str=None, **params):
        self.logger = get_logger(f"RandomForestClassifierModel.{name}")
        self.logger.info(f"Initializing Random Forest Classifier model with name {name} and params: {params}")
        super().__init__(name, **params)
        self.model = None
        self.params = params

    def build(self):
        self.model = RandomForestClassifier(**self.params)
        self.logger.info(f"Built Random Forest Classifier model with params: {self.params}")
        return self