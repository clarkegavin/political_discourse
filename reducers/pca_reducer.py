from logs.logger import get_logger
from sklearn.decomposition import PCA

class PCA_Reducer:
    """
    PCA Dimensionality Reduction.
    """

    def __init__(self, name: str=None, **params):
        self.logger = get_logger(f"PCA_Reducer.{name}")
        self.logger.info(f"Initializing PCA reducer with name {name} and params: {params}")
        self.name = name
        self.params = params
        self.model = None

    def build(self):
        self.model = PCA(**self.params)
        self.logger.info(f"Built PCA reducer with params: {self.params}")
        return self

    def fit_transform(self, X):
        if self.model is None:
            self.build()
        self.logger.info("Fitting and transforming data using PCA reducer")
        return self.model.fit_transform(X)

    def transform(self, X):
        if self.model is None:
            self.build()
        self.logger.info("Transforming data using PCA reducer")
        return self.model.transform(X)

    def fit(self, X):
        if self.model is None:
            self.build()
        self.logger.info("Fitting PCA reducer to data")
        self.model.fit(X)
        return self
