#vectorizers.__init__.py
from .base import Vectorizer
from .bert_vectorizer import BERTVectorizer
from .tfidf_vectorizer import TFIDFTextVectorizer
from .word2vec_vectorizer import Word2VecVectorizer
from .count_vectorizer import CountVectorizerWrapper
from .factory import VectorizerFactory

VectorizerFactory.register_vectorizer('tfidf', TFIDFTextVectorizer)
VectorizerFactory.register_vectorizer('word2vec', Word2VecVectorizer)
VectorizerFactory.register_vectorizer('bert', BERTVectorizer)
VectorizerFactory.register_vectorizer('count', CountVectorizerWrapper)
VectorizerFactory.register_vectorizer('CountVectorizer', CountVectorizerWrapper)
VectorizerFactory.register_vectorizer('countvectorizer', CountVectorizerWrapper)

__all__ = [
    "Vectorizer",
    "BERTVectorizer",
    "TFIDFTextVectorizer",
    "Word2VecVectorizer",
    "CountVectorizerWrapper",
    "VectorizerFactory",
]
