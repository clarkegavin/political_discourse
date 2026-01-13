from .base import Model
from sklearn.naive_bayes import GaussianNB, MultinomialNB, BernoulliNB
from typing import Dict, Any
from logs.logger import get_logger

class NaiveBayesClassificationModel(Model):
    """
    Naive Bayes Classification Model.
    """

    def __init__(self, name: str=None, **params):
        self.logger = get_logger(f"NaiveBayesClassificationModel.{name}")
        self.logger.info(f"Initializing Naive Bayes model with name {name} and params: {params}")
        super().__init__(name, **params)
        self.model = None
        self.params = params

    def build(self):
        self.model = MultinomialNB(**self.params)
        self.logger.info(f"Built Naive Bayes model with params: {self.params}")
        return self

    # def to_dict(self) -> Dict[str, Any]:
    #     """
    #     Serialize configuration required to recreate this model.
    #     Avoid serializing the fitted sklearn object itself.
    #     """
    #     return {
    #         "class": self.__class__.__name__,
    #         "params": {
    #             "alpha": self.alpha,
    #             "fit_prior": self.fit_prior,
    #             "class_prior": self.class_prior,
    #         },
    #     }