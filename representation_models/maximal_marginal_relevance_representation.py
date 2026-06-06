from bertopic.representation import MaximalMarginalRelevance
from .base import RepresentationModel
from .factory import RepresentationModelFactory
from logs.logger import get_logger

class MMRRepresentation(RepresentationModel):
    """Representation model using Maximal Marginal Relevance (MMR) approach."""

    def __init__(self, name: str=None, **params):
        self.name = name
        self.params = params
        self.representation_model = None
        self.logger = get_logger(f"MMRRepresentation.{name}")
        self.logger.info(f"Initialized MMRRepresentation with name {name} and params: {params}")

    def build(self):
        self.representation_model = MaximalMarginalRelevance(**self.params)
        self.logger.info(f"Built MMR representation model with params: {self.params}")
        return self

    def fit(self, X):
        """Fit the MMR model to the data."""
        self.model.fit(X)

    def transform(self, X):
        """Transform the data using the fitted MMR model."""
        return self.model.transform(X)

    def fit_transform(self, X):
        """Fit the model and transform the data."""
        self.fit(X)
        return self.transform(X)