# embedding_models/factory.py
from logs.logger import get_logger

class EmbeddingModelFactory:
    """
    Factory for managing embedding models.
    """
    _registry = {}
    logger = get_logger("EmbeddingModelFactory")

    @classmethod
    def register_embedding_model(cls, name: str, embedding_model_cls):
        if name in cls._registry:
            return
        cls._registry[name] = embedding_model_cls
        cls.logger.info(f"Registered embedding model: {name}")

    @classmethod
    def get_embedding_model(cls, name: str, **kwargs):
        cls.logger.info(f"Retrieving embedding model class for name: {name}")
        embedding_model_cls = cls._registry.get(name)

        if not embedding_model_cls:
            cls.logger.warning(f"Embedding model '{name}' not found in registry")
            return None

        # Convert ngram_range from list to tuple if needed
        if "ngram_range" in kwargs and isinstance(kwargs["ngram_range"], list):
            kwargs["ngram_range"] = tuple(kwargs["ngram_range"])

        cls.logger.info(f"Instantiating embedding model '{name}' with kwargs: {kwargs}")
        return embedding_model_cls(name=name, **kwargs)
