from bertopic.representation import KeyBERTInspired
from .base import RepresentationModel
from .factory import RepresentationModelFactory
from logs.logger import get_logger

class KeyBERTRepresentation(RepresentationModel):
    """Representation model using KeyBERT-inspired approach."""

    def __init__(self, name: str=None, **params):
        self.name = name
        self.params = params
        self.representation_model = None
        self.logger = get_logger(f"KeyBERTRepresentation.{name}")
        self.logger.info(f"Initialized KeyBERTRepresentation with name {name} and params: {params}")

    def build(self):
        self.representation_model = KeyBERTInspired(**self.params)
        self.logger.info(f"Built KeyBERT-inspired representation model with params: {self.params}")
        return self

    def fit(self, X):
        """Fit the KeyBERT-inspired model to the data."""
        self.model.fit(X)

    def transform(self, X):
        """Transform the data using the fitted KeyBERT-inspired model."""
        return self.model.transform(X)

    def fit_transform(self, X):
        """Fit the model and transform the data."""
        self.fit(X)
        return self.transform(X)