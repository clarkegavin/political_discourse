from sklearn.cluster import MiniBatchKMeans
from .base import Model
from logs.logger import get_logger
import numpy as np

class KMeansMiniClusterer(Model):
    """Simple wrapper around sklearn.cluster.MiniBatchKMeans to match project Model API."""

    def __init__(self, name: str = None, **params):
        super().__init__(name, **params)
        self.logger = get_logger(f"KMeansMiniClusterer.{name}")
        self.logger.info(f"Initializing MiniBatchKMeans clusterer with name={name} and params={params}")

    def build(self):
        # sklearn MiniBatchKMeans accepts n_clusters, random_state etc. We pass params directly.
        self.logger.info(f"Building MiniBatchKMeans model with params={self.params}")
        self.model = MiniBatchKMeans(**self.params)
        self.logger.info(f"Built MiniBatchKMeans model with params={self.params}")
        return self

    def fit(self, X, y=None):
        self.logger.info("Fitting MiniBatchKMeans model")
        if self.model is None:
            self.build()
        self.model.fit(X)
        return self

    def fit_predict(self, X, y=None, X_test=None):
        #change object to category
        self.logger.info("Fitting MiniBatchKMeans model and predicting labels")
        self.logger.info(f"MiniBatchKMeans Input data shape: {X.shape}")

        # Convert to numpy
        if isinstance(X, np.ndarray):
            self.logger.info(f"MiniBatchKMeans Input data is already numpy array with dtype: {X.dtype} ")
        else:
            X = X.to_numpy(dtype=np.float32, copy=False)

        self.logger.info(f"MiniBatchKMeans Converted data type: {X.dtype} ")
        if self.model is None:
            self.build()
        labels = self.model.fit_predict(X)
        return labels