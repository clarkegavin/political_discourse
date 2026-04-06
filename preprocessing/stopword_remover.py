from typing import Iterable, List, Optional, Set, Any
from .base import Preprocessor
from logs.logger import get_logger
import pandas as pd
import string

# dynamic imports
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

    DEFAULT_STOPWORDS: Set[str] = {
        "the", "and", "is", "in", "it", "of", "to", "a", "for", "on", "with", "that", "this",
        "as", "are", "was", "at", "by", "an", "be", "from", "or", "not", "but", "all",
        "if", "they", "you", "he", "she", "we", "his", "her", "its", "my", "your", "their",
        "what", "which", "when", "where", "who", "how", "there", "so", "no", "yes", "do",
        "does", "did", "have", "has", "had", "will", "would", "can", "could", "should",
        "i", "me", "us", "them", "our", "yours", "theirs", "asked"
    }

    PROCEDURAL_STOPWORDS: Set[str] = {
        "minister", "department", "government", "office", "agency", "bureau", "commission",
        "council", "committee", "secretary", "director", "manager", "head", "leader", "chief", "executive", "administrator", "official",
        "deputy", "assistant", "associate", "vice", "president", "prime",
        "king", "queen", "emperor", "empress", "duke", "duchess",
        "prince", "princess", "lord", "lady", "sir", "madam",
        "mr", "mrs", "ms", "miss", "dr", "professor", "prof",
        "engineer", "scientist", "researcher", "analyst",
        "consultant", "advisor", "counselor", "attorney", "lawyer",
        "judge", "justice", "clerk", "treasurer", "auditor",
        "accountant", "officer", "agent", "representative",
        "case", "possible", "respond", "directly", "matter", "service"
    }

    def __init__(
        self,
        columns: Optional[List[str]] = None,
        language: str = "english",
        stopwords: Optional[Iterable[str]] = None,
        lower: bool = True,
    ):
        self.logger = get_logger(self.__class__.__name__)

        self.columns = columns
        self.language = language
        self.lower = bool(lower)

        self.logger.info(
            f"Initializing StopwordRemover(columns={columns}, language={language}, lower={self.lower})"
        )

        # Build stopword set
        if stopwords is not None:
            try:
                base = set(stopwords)
                self.stopwords = base | self.DEFAULT_STOPWORDS | self.PROCEDURAL_STOPWORDS
                self.logger.info("Using explicit + default + procedural stopwords")
            except Exception:
                self.logger.warning("Invalid stopwords provided; falling back to defaults")
                self.stopwords = self.DEFAULT_STOPWORDS | self.PROCEDURAL_STOPWORDS

        else:
            if _nltk_stopwords is not None:
                try:
                    base = set(_nltk_stopwords.words(self.language))
                    self.stopwords = base | self.PROCEDURAL_STOPWORDS
                    self.logger.info(f"Loaded NLTK stopwords ({len(base)})")
                except Exception:
                    self.logger.warning("NLTK stopwords unavailable; using defaults")
                    self.stopwords = self.DEFAULT_STOPWORDS | self.PROCEDURAL_STOPWORDS
            else:
                self.logger.warning("NLTK not available; using defaults")
                self.stopwords = self.DEFAULT_STOPWORDS | self.PROCEDURAL_STOPWORDS

        # tokenizer (optional)
        self._tokenize = _word_tokenize if _word_tokenize else None

    def fit(self, X: Any):
        return self

    def _clean_text(self, text: Any) -> Any:
        if not isinstance(text, str):
            return text

        s = text.lower() if self.lower else text

        # remove punctuation
        s = s.translate(str.maketrans("", "", string.punctuation))

        # tokenize
        tokens = self._tokenize(s) if self._tokenize else s.split()

        # remove stopwords
        filtered = [t for t in tokens if t not in self.stopwords]

        return " ".join(filtered)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        target_columns = (
            self.columns
            if self.columns is not None
            else df.select_dtypes(include=["object", "string"]).columns
        )

        self.logger.info(f"Applying StopwordRemover to columns: {list(target_columns)}")

        for col in target_columns:
            if col not in df.columns:
                self.logger.warning(f"Column '{col}' not found, skipping")
                continue

            df[col] = df[col].apply(self._clean_text)

        return df

    def get_params(self) -> dict:
        return {
            "columns": self.columns,
            "language": self.language,
            "lower": self.lower,
            "stopwords_count": len(self.stopwords),
        }