# ctfidf_models/class_tfidf.py
from bertopic.vectorizers import ClassTfidfTransformer
from logs.logger import get_logger

class ClassTfidfModel:
    """Wrapper to expose a consistent interface for ClassTfidfTransformer.

    This wrapper mirrors the simple factory pattern used by other model factories
    and provides a build() method returning the underlying transformer.
    """
    def __init__(self, name: str = "class_tfidf", **params):
        self.name = name
        self.params = params or {}
        self.logger = get_logger(self.__class__.__name__)
        self.model = None

    def build(self):
        # ClassTfidfTransformer in bertopic accepts sparse/dense matrix input but is typically used
        # via BERTopic's pipeline. We keep a thin wrapper to follow project factory patterns.
        self.logger.info(f"Building ClassTfidfTransformer with params: {self.params}")
        # Pass params through directly; if ngram_range is list, convert to tuple
        if "ngram_range" in self.params and isinstance(self.params["ngram_range"], list):
            self.params["ngram_range"] = tuple(self.params["ngram_range"])

        self.model = ClassTfidfTransformer(**self.params)
        return self.model

    # Keep compatibility if callers expect .model attribute
    def __repr__(self):
        return f"<ClassTfidfModel name={self.name} params={self.params}>"

