from typing import Iterable, Optional, Set
import importlib

from stopwords.defaults import (
    DEFAULT_STOPWORDS,
    PROCEDURAL_STOPWORDS,
)

# dynamic nltk import
try:
    _nltk_corpus = importlib.import_module("nltk.corpus")
    _nltk_stopwords = getattr(_nltk_corpus, "stopwords", None)
except Exception:
    _nltk_stopwords = None
    

class StopwordProvider:
    @staticmethod
    def get_stopwords(
            language: str = "english",
            include_nltk: bool = True,
            include_default: bool = True,
            include_procedural: bool = True,
            additional_stopwords: Optional[Iterable[str]] = None,
    ) -> Set[str]:

        stopwords = set()

        # nltk
        if include_nltk and _nltk_stopwords is not None:
            try:
                stopwords |= set(_nltk_stopwords.words(language))
            except Exception:
                pass

        # defaults
        if include_default:
            stopwords |= DEFAULT_STOPWORDS

        # procedural
        if include_procedural:
            stopwords |= PROCEDURAL_STOPWORDS

        # user additions
        if additional_stopwords:
            stopwords |= set(additional_stopwords)

        return stopwords