# preprocessing/stopword_remover.py
from typing import Iterable, List, Optional, Set, Any
from .base import Preprocessor
from logs.logger import get_logger
import pandas as pd
import string
import re

# dynamic imports to avoid hard dependency at import time
try:
    import importlib
    _nltk_corpus = importlib.import_module("nltk.corpus")
    _nltk_tokenize = importlib.import_module("nltk.tokenize")
    _nltk_stopwords = getattr(_nltk_corpus, "stopwords", None)
    _word_tokenize = getattr(_nltk_tokenize, "word_tokenize", None)
except Exception:
    _nltk_stopwords = None
    _word_tokenize = None


class StopwordRemover(Preprocessor):
    """Removes stopwords from text.

    Parameters
    ----------
    language: str
        Language for stopword lookup (default: "english").
    stopwords: Optional[Iterable[str]]
        Optional explicit stopword list to use. If provided this takes precedence
        over library-provided stopwords.
    lower: bool
        If True, text is lowercased prior to stopword removal.

    Behaviour
    ---------
    - If NLTK stopwords are available they will be used (unless explicit list
      is provided). If not available and no explicit list provided a small
      default stopword set will be used.
    - Tokenization uses NLTK's word_tokenize when available, otherwise a
      simple split() is used.
    """

    DEFAULT_STOPWORDS: Set[str] = {
        "the", "and", "is", "in", "it", "of", "to", "a", "for", "on", "with", "that", "this",
        "as", "are", "was", "at", "by", "an", "be", "from", "or", "not", "but", "all",
        "if", "they", "you", "he", "she", "we", "his", "her", "its", "my", "your", "their",
        "what", "which", "when", "where", "who", "how", "there", "so", "no", "yes", "do",
        "does", "did", "have", "has", "had", "will", "would", "can", "could", "should",
        "i", "me", "us", "them", "our", "yours", "theirs"
    }

    ROBLOX_STOPWORDS: Set[str] = {
        "game", "play", "playing", "plays", "fun", "awesome", "cool", "best", "epic",
        "new", "update", "updates", "updated", "soon", "like", "likes", "favorite",
        "favorites", "fav", "follow", "join", "visit", "check", "share", "welcome",
        "enjoy", "thanks", "thank", "please", "plz", "pls", "blox", "roblox", "robux",
        "join", "fun", "welcome", "good luck", "favorite", "best", "group", "unlock",
        "will", "free"
    }

    def __init__(self, field: str, language: str = "english",
                 stopwords: Optional[Iterable[str]] = None, lower: bool = True):

        if not field:
            raise ValueError("'field' parameter is required for StopwordRemover")

        self.logger = get_logger(self.__class__.__name__)
        self.field = field
        self.language = language
        self.lower = bool(lower)
        self.logger.info(f"Initializing StopwordRemover language={language} lower={self.lower}")

        # determine stopword set
        if stopwords is not None:
            try:
                self.stopwords = set(s for s in stopwords if s is not None)
                self.logger.info("Using explicit stopword list provided in params")
            except Exception:
                self.logger.warning("Provided stopwords not iterable; falling back to defaults")
                #self.stopwords = set(self.DEFAULT_STOPWORDS)
                self.stopwords = set(self.DEFAULT_STOPWORDS).union(self.ROBLOX_STOPWORDS)
        else:
            # try to load from nltk if available
            if _nltk_stopwords is not None:
                try:
                    #self.stopwords = set(_nltk_stopwords.words(self.language))
                    #base_sw = set(_nltk_stopwords.words(self.language))
                    base_sw = set(_nltk_stopwords.words(self.language)) if _nltk_stopwords else set()
                    self.stopwords = base_sw.union(self.ROBLOX_STOPWORDS)
                    self.logger.info(f"Loaded {len(self.stopwords)} stopwords for language '{self.language}' from NLTK")
                except Exception:
                    self.logger.warning(f"NLTK stopwords for '{self.language}' not available; using default set")
                    self.stopwords = set(self.DEFAULT_STOPWORDS).union(self.ROBLOX_STOPWORDS)
            else:
                self.logger.warning("NLTK stopwords not available; using small built-in default set")
                self.stopwords = set(self.DEFAULT_STOPWORDS).union(self.ROBLOX_STOPWORDS)

        # choose tokenizer
        self._tokenize = _word_tokenize if _word_tokenize is not None else None

        self.logger.info(f"Stopwords: {sorted(list(self.stopwords))}")
        self.logger.info(
            f"Initialized StopwordRemover(field={field}, language={language}, lower={self.lower})"
        )

    def fit(self, X: Iterable[str]):
        # stateless
        return self

    def _clean_text(self, text: str) -> str:
        """Internal utility to remove stopwords from one string."""
        # if text is None:
        #     return ""
        #
        # s = str(text)
        # if self.lower:
        #     s = s.lower()
        #
        # # Use NLTK tokenizer if available, else fallback
        # try:
        #     tokens = self._tokenize(s) if self._tokenize else s.split()
        # except Exception:
        #     tokens = s.split()
        #
        # # Strip punctuation and filter stopwords
        # cleaned_tokens = []
        # for t in tokens:
        #     # remove surrounding punctuation
        #     t_clean = t.strip(string.punctuation)
        #     # remove extra repeated punctuation inside word
        #     t_clean = re.sub(r'[!?.,]{2,}', '', t_clean)
        #     if t_clean and t_clean not in self.stopwords:
        #         cleaned_tokens.append(t_clean)
        #
        # return " ".join(cleaned_tokens)
        if text is None:
            return ""

            # Convert to string and lowercase if needed
        s = str(text)
        if self.lower:
            s = s.lower()

        # Remove punctuation
        s = s.translate(str.maketrans("", "", string.punctuation))

        # Tokenize (simple whitespace split is enough after punctuation removed)
        tokens = s.split()

        # Remove stopwords
        filtered = [t for t in tokens if t not in self.stopwords]

        return " ".join(filtered)

    def transform(self, X: Iterable[Any]) -> Any:
        self.logger.info(f"Applying StopwordRemover on field '{self.field}'")

        # pandas.DataFrame path (main)
        if pd is not None and isinstance(X, pd.DataFrame):
            df = X.copy()

            if self.field not in df.columns:
                self.logger.warning(
                    f"Field '{self.field}' not present in DataFrame; returning original DataFrame"
                )
                return df

            df[self.field] = df[self.field].apply(self._clean_text)

            self.logger.info("Completed StopwordRemover on DataFrame")
            return df

        # terable path (fallback)
        out = [self._clean_text(item) for item in X]

        self.logger.info("Completed StopwordRemover on iterable")
        return out

    def get_params(self) -> dict:
        return {
            "field": self.field,
            "language": self.language,
            "lower": self.lower,
            "stopwords_count": len(self.stopwords),
        }
