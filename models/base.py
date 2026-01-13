#model/base.py
import os
from abc import ABC, abstractmethod
from logs.logger import get_logger
import mlflow.sklearn
import joblib


class Model(ABC):
    """
    Abstract base class for data models.
    """

    logger = get_logger("ModelBase")

    def __init__(self, name: str, **params):
        self.logger = get_logger(f"Initializing Base Model.{name}")
        self.name = name
        self.params = params
        self.model = None
        self.logger.info(f"Initialized model: {self.name} with params: {self.params}")

    @abstractmethod
    def build(self):
        """
        Build or initialize the model.
        """
        pass

    def fit(self, X_train, y_train):
        """
        Fit the model to training data.
        """
        self.logger.info(f"Fitting model: {self.name}")
        if self.model is None:
            self.build()
        self.model.fit(X_train, y_train)
        return self

    def predict(self, X_test):
        """
        Make predictions using the model.
        """
        self.logger.info(f"Making predictions with model: {self.name}")
        if self.model is None:
            self.logger.error("Model has not been built or fitted yet.")
        return self.model.predict(X_test)

    def fit_predict(self, X_train, y_train, X_test):
        """
        Fit the model and make predictions in one step.
        """
        self.fit(X_train, y_train)
        return self.predict(X_test)

    def save(self, path: str=None):
        """
        Save the model to a file.
        """
        mlflow.sklearn.log_model(self.model, path or self.name)
        if path:
            self.logger.info(f"Model saved to {path}")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            joblib.dump(self.model, path)

        self.logger.info(f"Model saved with name: {self.name} in MLflow")


    # @abstractmethod
    # def to_dict(self):
    #     """
    #     Convert the model instance to a dictionary.
    #     """
    #     pass