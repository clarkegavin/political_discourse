# vectorizers/word2vec_vectorizer.py
import numpy as np
from gensim.models import Word2Vec
from .base import Vectorizer

class Word2VecVectorizer(Vectorizer):
    def __init__(self, name: str, column: str, vector_size=300, window=5, min_count=2, **kwargs):
        self.name = name
        self.column = column
        self.vector_size = vector_size
        self.model = Word2Vec(
            vector_size=vector_size,
            window=window,
            min_count=min_count,
            **kwargs
        )

    def fit(self, X):
        sentences = [t.split() for t in X[self.column]]
        self.model.build_vocab(sentences)
        self.model.train(sentences, total_examples=len(sentences), epochs=10)

    def transform(self, X):
        def embed(sentence):
            tokens = sentence.split()
            vectors = [self.model.wv[w] for w in tokens if w in self.model.wv]
            return np.mean(vectors, axis=0) if vectors else np.zeros(self.vector_size)

        return np.vstack([embed(t) for t in X[self.column]])

    def get_feature_names(self):
        return [f"w2v_dim_{i}" for i in range(self.vector_size)]
