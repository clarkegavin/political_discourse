#models/hdbscan_clusterer.py
import hdbscan
#from sklearn.cluster import HDBSCAN
from .base import Model
from logs.logger import get_logger

class HDBSCANClusterer(Model):
    def __init__(self, name: str = None, **params):
        super().__init__(name, **params)
        self.logger = get_logger(f"HDBSCANClusterer.{name}")
        self.logger.info(f"Initializing HDBSCAN clusterer with name={name} and params={params}")
        self.probabilities_ = None
        self.outlier_scores_ = None
        self.cluster_persistence_ = None

    def build(self):
        self.model = hdbscan.HDBSCAN(**self.params)
        #self.model = HDBSCAN(**self.params)
        self.logger.info(f"Built HDBSCAN model with params={self.params}")
        return self

    #overriding base fit and fit_predict methods because HDBSCAN is unsupervised and does not use y labels
    def fit(self, X, y=None):
        if self.model is None:
            self.build()
        self.model.fit(X)
        return self

    def fit_predict(self, X, y=None, X_test=None):

        if self.model is None:
            self.build()

        labels = self.model.fit_predict(X)

        # expose HDBSCAN - specific attributes
        self.probabilities_ = self.model.probabilities_
        self.outlier_scores_ = self.model.outlier_scores_
        self.cluster_persistence_ = getattr(self.model, "cluster_persistence_", None)

        return labels
