from sklearn.linear_model import LogisticRegression
from logs.logger import get_logger
from .base import Model

class LogisticRegressionModel(Model):
    """
    Logistic Regression Model.
    """

    def __init__(self, name: str=None, **params):
        self.logger = get_logger(f"LogisticRegressionModel.{name}")
        self.logger.info(f"Initializing Logistic Regression model with name {name} and params: {params}")
        super().__init__(name, **params)
        self.model = None
        self.params = params

    def build(self):
        self.model = LogisticRegression(**self.params)
        self.logger.info(f"Built Logistic Regression model with params: {self.params}")
        return self