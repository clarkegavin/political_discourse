# vectorizers/tfidf_vectorizer.py
from sklearn.feature_extraction.text import TfidfVectorizer
from .base import Vectorizer

class TFIDFTextVectorizer(Vectorizer):
    def __init__(self, name: str, column: str, **params):
        self.name = name
        self.column = column
        self.vectorizer = TfidfVectorizer(**params)

    def fit(self, X):
        self.vectorizer.fit(X[self.column])

    def transform(self, X):
        return self.vectorizer.transform(X[self.column])

    def get_feature_names(self):
        return self.vectorizer.get_feature_names_out()