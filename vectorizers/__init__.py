#vectorizers.__init__.py
from .base import Vectorizer
from .bert_vectorizer import BERTVectorizer
from .tfidf_vectorizer import TFIDFTextVectorizer
from .word2vec_vectorizer import Word2VecVectorizer
from .factory import VectorizerFactory

VectorizerFactory.register_vectorizer('tfidf', TFIDFTextVectorizer)
VectorizerFactory.register_vectorizer('word2vec', Word2VecVectorizer)
VectorizerFactory.register_vectorizer('bert', BERTVectorizer)

__all__ = [
    "Vectorizer",
    "BERTVectorizer",
    "TFIDFTextVectorizer",
    "Word2VecVectorizer",
    "VectorizerFactory",
]
