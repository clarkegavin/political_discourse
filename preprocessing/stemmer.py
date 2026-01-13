# preprocessing/stemmer.py
from typing import Iterable, List, Any
from .base import Preprocessor
from logs.logger import get_logger

# Dynamically import heavy NLP libraries to avoid static analysis errors when
# they are not installed in the environment used by the editor/CI.
try:
    import importlib
    _nltk_stem = importlib.import_module("nltk.stem")
    _nltk_tokenize = importlib.import_module("nltk.tokenize")
    SnowballStemmer: Any = getattr(_nltk_stem, "SnowballStemmer", None)
    PorterStemmer: Any = getattr(_nltk_stem, "PorterStemmer", None)
    word_tokenize: Any = getattr(_nltk_tokenize, "word_tokenize", None)
except Exception:
    SnowballStemmer = None
    PorterStemmer = None
    word_tokenize = None


class Stemmer(Preprocessor):
    """Configurable stemmer supporting 'snowball' and 'porter' algorithms.

    Parameters
    ----------
    algorithm: str
        One of 'snowball' (default) or 'porter'.
    language: str
        Language name passed to SnowballStemmer (ignored by PorterStemmer).

    Behaviour
    ---------
    - If required NLTK components are missing the transformer becomes a no-op.
    - Tokenization uses NLTK's word_tokenize when available, else simple split().
    """

    def __init__(self, algorithm: str = "snowball", language: str = "english"):
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info(f"Initializing Stemmer algorithm={algorithm} language={language}")
        self.algorithm = (algorithm or "snowball").strip().lower()
        self.language = language
        self._stemmer = None
        self._tokenize = word_tokenize

        if self.algorithm in ("snowball", "snowballstemmer"):
            if SnowballStemmer is None:
                self.logger.warning("NLTK SnowballStemmer not available; Stemmer will be a no-op")
            else:
                try:
                    self._stemmer = SnowballStemmer(self.language)
                except Exception as e:
                    self._stemmer = None
                    self.logger.warning(f"Failed to initialize SnowballStemmer: {e}; will be no-op")

        elif self.algorithm in ("porter", "porterstemmer"):
            if PorterStemmer is None:
                self.logger.warning("NLTK PorterStemmer not available; Stemmer will be a no-op")
            else:
                try:
                    # PorterStemmer does not accept language
                    self._stemmer = PorterStemmer()
                except Exception as e:
                    self._stemmer = None
                    self.logger.warning(f"Failed to initialize PorterStemmer: {e}; will be no-op")
        else:
            self.logger.warning(f"Unknown algorithm '{self.algorithm}'; Stemmer will be a no-op")

    def fit(self, X: Iterable[str]):
        # stateless
        return self

    def transform(self, X: Iterable[str]) -> List[str]:
        self.logger.info("Applying stemming transformation")
        if self._stemmer is None:
            self.logger.warning("Stemmer not initialized properly; returning input unchanged")
            return [str(x) for x in X]

        out: List[str] = []
        for i, doc in enumerate(X):
            s = str(doc) if doc is not None else ""
            try:
                tokens = self._tokenize(s) if self._tokenize is not None else s.split()
            except Exception:
                tokens = s.split()

            # apply stem method generically
            try:
                stemmed_tokens = [self._stemmer.stem(t) for t in tokens]
            except Exception as e:
                self.logger.warning(f"Stemming failed on doc index {i}: {e}; returning original doc")
                out.append(s)
                continue

            stemmed_doc = " ".join(stemmed_tokens)
            out.append(stemmed_doc)

            # # Log first few examples to check
            # if i < 5:  # log first 5 rows only
            #     try:
            #         self.logger.info(f"Original: {s.encode('utf-8', errors='ignore')}")
            #         self.logger.info(f"Stemmed : {stemmed_doc.encode('utf-8', errors='ignore')}")
            #     except Exception:
            #         pass

        return out

    def get_params(self):
        return {"stemmer_type": self.algorithm, "language": self.language}