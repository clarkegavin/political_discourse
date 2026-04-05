from .base import EmbeddingModel
from .factory import EmbeddingModelFactory

#register embedding models here
from .bert_embedder import BERTEmbeddingModel
EmbeddingModelFactory.register_embedding_model("bert", BERTEmbeddingModel)

__all__ = [
    "EmbeddingModel",
    "EmbeddingModelFactory",
]