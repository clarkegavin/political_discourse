# preprocessing/lemmatizer.py
from typing import Iterable, List
from .base import Preprocessor
from logs.logger import get_logger

# dynamic import of spaCy to avoid hard dependency at import time
try:
    import importlib
    spacy = importlib.import_module("spacy")
except Exception:
    spacy = None


class Lemmatizer(Preprocessor):
    """Lemmatizer that uses spaCy if available; otherwise no-op.

    Note: spaCy models are not included by default. This class will log a warning
    and behave as identity transform when spaCy or models are missing.
    """

    def __init__(self, model: str = "en_core_web_sm", columns: List[str] = None):
        super().__init__(columns)
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info(f"Initializing Lemmatizer with model='{model}'")
        self.model = model
        self.nlp = None

        if spacy is None:
            self.logger.warning("spaCy not available; Lemmatizer will be a no-op")
        else:
            try:
                self.nlp = spacy.load(model)
            except Exception:
                self.logger.warning(f"spaCy model '{model}' not available; Lemmatizer will be a no-op")
                self.nlp = None

    def fit(self, X: Iterable[str]):
        return self

    def transform(self, X):

        if self.columns is None:
            columns = X.columns
        else:
            columns = self.columns

        X = X.copy()

        for column in columns:

            self.logger.info(
                f"Lemmatizing column '{column}'"
            )

            X[column] = self._lemmatize_series(
                X[column]
            )

        return X

    def _lemmatize_series(self, series):

        if self.nlp is None:
            return series

        texts = series.fillna("").tolist()

        output = []

        with self.nlp.select_pipes(
                disable=["ner", "parser"]
        ):
            for doc in self.nlp.pipe(
                    texts,
                    batch_size=50
            ):
                output.append(
                    " ".join(
                        token.lemma_
                        for token in doc
                    )
                )

        return output

    def get_params(self) -> dict:
        return {"model": self.model}