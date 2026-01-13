# vectorizers/base_vectorizer.py
from abc import ABC, abstractmethod

class Vectorizer(ABC):

    @abstractmethod
    def fit(self, X):
        pass

    @abstractmethod
    def transform(self, X):
        pass

    @abstractmethod
    def get_feature_names(self):
        pass

    def fit_transform(self, X):
        self.fit(X)
        return self.transform(X)

