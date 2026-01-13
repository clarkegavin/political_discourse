from .base import Model
from logs.logger import get_logger
from sklearn.svm import SVC

class SVMModel(Model):
    """
    Support Vector Machine Linear Model.
    """

    def __init__(self, name: str=None, **params):
        self.logger = get_logger(f"SVMModel.{name}")
        self.logger.info(f"Initializing SVM  model with name {name} and params: {params}")
        super().__init__(name, **params)
        self.model = None
        self.params = params

    def build(self):
        self.model = SVC(**self.params)
        self.logger.info(f"Built SVM model with params: {self.params}")
        return self