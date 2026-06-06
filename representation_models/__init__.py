from .factory import RepresentationModelFactory
from .key_bert_representation import KeyBERTRepresentation
from .maximal_marginal_relevance_representation import MMRRepresentation

RepresentationModelFactory.register('keybert', KeyBERTRepresentation )
RepresentationModelFactory.register('mmr', MMRRepresentation)

__all__ = [
    "RepresentationModelFactory",
    "KeyBERTRepresentation",
    "MMRRepresentation",
]