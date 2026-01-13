# models/agglomerative_clusterer.py
from sklearn.cluster import AgglomerativeClustering
from .base import Model
from logs.logger import get_logger
import numpy as np

class AgglomerativeClusterer(Model):
    """Wrapper around sklearn.cluster.AgglomerativeClustering matching project Model API."""

    def __init__(self, name: str = None, **params):
        super().__init__(name, **params)
        self.logger = get_logger(f"AgglomerativeClusterer.{name}")
        self.logger.info(f"Initializing AgglomerativeClusterer with name={name} and params={params}")

    def build(self):
        self.logger.info(f"Building AgglomerativeClustering model with params={self.params}")
        # sklearn AgglomerativeClustering accepts n_clusters, affinity/metric, linkage, etc.
        # Pass params directly; user is responsible for correct params.
        self.model = AgglomerativeClustering(**self.params)
        self.logger.info("Built AgglomerativeClustering model")
        return self

    def fit(self, X, y=None):
        self.logger.info("Fitting AgglomerativeClustering model")
        if self.model is None:
            self.build()
        # AgglomerativeClustering has fit method
        self.model.fit(X)
        return self

    def fit_predict(self, X, y=None, X_test=None):
        self.logger.info("Fitting AgglomerativeClustering model and predicting labels")
        self.logger.info(f"Agglomerative Input data shape: {getattr(X, 'shape', None)}")

        # Convert to numpy
        if isinstance(X, np.ndarray):
            self.logger.info(f"Agglomerative Input data is already numpy array with dtype: {X.dtype} ")
        else:
            X = X.to_numpy(dtype=np.float32, copy=False)
        self.logger.info(f"Agglomerative Converted data type: {X.dtype} ")

        if self.model is None:
            self.build()

        # use fit_predict if available
        if hasattr(self.model, 'fit_predict'):
            labels = self.model.fit_predict(X)
        else:
            # fallback: fit then predict (AgglomerativeClustering doesn't have predict in sklearn)
            self.model.fit(X)
            try:
                labels = self.model.labels_
            except Exception:
                labels = None
        return labels

