from .base import Model
from logs.logger import get_logger
from xgboost import XGBClassifier

class XGBoostClassificationModel(Model):
    """
    XGBoost Classification Model.
    """

    def __init__(self, name: str=None, **params):
        self.logger = get_logger(f"XGBoostClassificationModel.{name}")
        self.logger.info(f"Initializing XGBoost model with name {name} and params: {params}")
        super().__init__(name, **params)
        self.model = None
        self.params = params

    def build(self):
        self.model = XGBClassifier(**self.params)
        self.logger.info(f"Built XGBoost model with params: {self.params}")
        return self

    # Override fit to pass additional parameters if needed
    def fit(self, X, y, **kwargs):
        if self.model is None:
            self.build()
        # Pass all kwargs (like sample_weight) to XGBClassifier
        self.model.fit(X, y, **kwargs)
        return self