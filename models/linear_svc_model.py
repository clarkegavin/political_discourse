from sklearn.svm import LinearSVC
from logs.logger import get_logger
from .base import Model

class LinearSVCModel(Model):

    def __init__(self, name: str=None, **params):
        self.logger = get_logger("LinearSVCModel")
        self.logger.info(f"Initializing LinearSVC model")
        super().__init__(name, **params)
        self.model = None
        self.params = params


    def build(self):
        self.model = LinearSVC(**self.params)
        self.logger.info(f"Built LinearSVC model with params: {self.params}")
        return self