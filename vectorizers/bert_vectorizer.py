# vectorizers/bert_vectorizer.py
from sentence_transformers import SentenceTransformer
from .base import Vectorizer
import numpy as np
from logs.logger import get_logger

class BERTVectorizer(Vectorizer):
    def __init__(self, name: str, column: str, model_name="sentence-transformers/all-mpnet-base-v2"):
        self.name = name
        self.column = column
        self.model = SentenceTransformer(model_name)
        self.model_name = model_name
        self.logger = get_logger(self.__class__.__name__)
        self._dim = None  # to be set after fitting

    def fit(self, X):
        return  # no fitting

    def transform(self, X):
        self.logger.info(f"Transforming data using BERTVectorizer {self.model_name} on column '{self.column}'")
        return self.model.encode(X[self.column].tolist(), show_progress_bar=False)

    def get_feature_names(self):
        """Return placeholder feature names for pipeline compatibility."""
        if self._dim is None:
            return []  # not fit yet
        return np.array([f"bert_dim_{i}" for i in range(self._dim)])