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

    def __init__(self, model: str = "en_core_web_sm"):
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

    def transform(self, X: Iterable[str]) -> List[str]:
        self.logger.info("Starting Lemmatizer transformation")

        #log sample before transformation
        # sample_before = list(X)[:2]
        # self.logger.info(f"Sample before lemmatization: {sample_before}")

        if self.nlp is None:
            return list(X)
        out = []
        for doc in self.nlp.pipe(X, disable=["ner", "parser"]):
            out.append(" ".join(tok.lemma_ for tok in doc))
        self.logger.info("Completed Lemmatizer transformation")

        # sample_after = out[:2]
        # self.logger.info(f"Sample after lemmatization: {sample_after}")
        return out

    def get_params(self) -> dict:
        return {"model": self.model}