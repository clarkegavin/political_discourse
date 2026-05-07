# vectorizers/count_vectorizer.py
from sklearn.feature_extraction.text import CountVectorizer
from .base import Vectorizer
from typing import Optional, Any
import pandas as pd
import numpy as np

class CountVectorizerWrapper(Vectorizer):
    """Wrapper around sklearn CountVectorizer.

    Accepts optional `column` parameter for DataFrame-based workflows. If `column` is
    not provided, the wrapper expects X passed to fit/transform to be an iterable of
    strings (list/Series/ndarray) — this is suitable for BERTopic internals which pass
    raw documents.
    """
    def __init__(self, name: str, column: Optional[str] = None, **params: Any):
        self.name = name
        self.column = column
        # Convert any list ngram_range into tuple handled upstream by factory as well
        if "ngram_range" in params and isinstance(params["ngram_range"], list):
            params["ngram_range"] = tuple(params["ngram_range"])
        self.vectorizer = CountVectorizer(**params)

    def _extract_texts(self, X):
        # If a column is specified and X is DataFrame-like, use that column
        if self.column is not None and hasattr(X, "__getitem__"):
            try:
                return X[self.column].fillna("").tolist()
            except Exception:
                # Fall back to attempting to treat X as an iterable
                pass

        # If X is a pandas Series
        if isinstance(X, pd.Series):
            return X.fillna("").tolist()

        # If X is list/tuple/ndarray of strings
        if isinstance(X, (list, tuple, np.ndarray)):
            # convert None -> empty string
            return [("" if v is None else v) for v in X]

        # As a last resort, try to iterate over X
        try:
            return [str(v) if v is not None else "" for v in X]
        except Exception:
            raise TypeError("Unsupported input type for CountVectorizerWrapper.transform/fit")

    def fit(self, X):
        texts = self._extract_texts(X)
        self.vectorizer.fit(texts)

    def transform(self, X):
        texts = self._extract_texts(X)
        return self.vectorizer.transform(texts)

    def fit_transform(self, X):
        texts = self._extract_texts(X)
        return self.vectorizer.fit_transform(texts)

    def get_feature_names(self):
        return self.vectorizer.get_feature_names_out()

