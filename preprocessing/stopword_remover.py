from typing import Iterable, List, Optional, Set, Any
from .base import Preprocessor
from logs.logger import get_logger
import pandas as pd
import string
from stopwords.provider import StopwordProvider

# dynamic imports
try:
    import importlib
    _nltk_tokenize = importlib.import_module("nltk.tokenize")
    _word_tokenize = getattr(_nltk_tokenize, "word_tokenize", None)
except Exception:
    _word_tokenize = None


class StopwordRemover(Preprocessor):

    def __init__(
        self,
        columns: Optional[List[str]] = None,
        language: str = "english",
        stopwords: Optional[Iterable[str]] = None,
        lower: bool = True,
        include_nltk: bool = True,
        include_defaults: bool = True,
        include_procedural: bool = True,
    ):
        self.logger = get_logger(self.__class__.__name__)

        self.columns = columns
        self.language = language
        self.lower = bool(lower)
        self.include_nltk = include_nltk
        self.include_defaults = include_defaults
        self.include_procedural = include_procedural

        self.logger.info(
            f"Initializing StopwordRemover(columns={columns}, language={language}, lower={self.lower})"
        )

        additional_stopwords = set(stopwords)
        self.stopwords = additional_stopwords | StopwordProvider.get_stopwords(
            language=language,
            include_nltk=include_nltk,
            include_defaults=include_defaults,
            include_procedural=include_procedural,
        )

        # tokenizer (optional)
        #self._tokenize = _word_tokenize if _word_tokenize else None
        self._tokenize = _word_tokenize or (lambda text: text.split())

    def fit(self, X: Any):
        return self

    def _clean_text(self, text: Any) -> Any:
        if not isinstance(text, str):
            return text

        s = text.lower() if self.lower else text

        # remove punctuation
        s = s.translate(str.maketrans("", "", string.punctuation))

        # tokenize
       # tokens = self._tokenize(s) if self._tokenize else s.split()
        tokens = self._tokenize(s)

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