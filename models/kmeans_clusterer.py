# models/kmeans_clusterer.py
from sklearn.cluster import KMeans
from .base import Model
from logs.logger import get_logger
import numpy as np

class KMeansClusterer(Model):
    """Simple wrapper around sklearn.cluster.KMeans to match project Model API."""

    def __init__(self, name: str = None, **params):
        super().__init__(name, **params)
        self.logger = get_logger(f"KMeansClusterer.{name}")
        self.logger.info(f"Initializing KMeans clusterer with name={name} and params={params}")

    def build(self):
        # sklearn KMeans accepts n_clusters, random_state etc. We pass params directly.
        self.logger.info(f"Building KMeans model with params={self.params}")
        self.model = KMeans(**self.params)
        self.logger.info(f"Built KMeans model with params={self.params}")
        return self

    def fit(self, X, y=None):
        self.logger.info("Fitting KMeans model")
        if self.model is None:
            self.build()
        self.model.fit(X)
        return self

    def fit_predict(self, X, y=None, X_test=None):
        #change object to category
        self.logger.info("Fitting KMeans model and predicting labels")
        self.logger.info(f"KMeans Input data shape: {X.shape}")

        # Convert to numpy
        if isinstance(X, np.ndarray):
            self.logger.info(f"MiniBatchKMeans Input data is already numpy array with dtype: {X.dtype} ")
        else:
            X = X.to_numpy(dtype=np.float32, copy=False)
        self.logger.info(f"KMeans Converted data type: {X.dtype} ")
        if self.model is None:
            self.build()
        labels = self.model.fit_predict(X)
        return labels

