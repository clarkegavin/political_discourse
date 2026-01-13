from sklearn.linear_model import LinearRegression
from logs.logger import get_logger
from .base import Model

class LinearRegressionModel(Model):
    """
    Linear Regression Model.
    """

    def __init__(self, name: str=None, **params):
        self.logger = get_logger(f"LinearRegressionModel.{name}")
        self.logger.info(f"Initializing Linear Regression model with name {name} and params: {params}")
        super().__init__(name, **params)
        self.model = None
        self.params = params

    def build(self):
        self.model = LinearRegression(**self.params)
        self.logger.info(f"Built Linear Regression model with params: {self.params}")
        return self