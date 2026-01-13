# vectorizers/factory.py
from logs.logger import get_logger

class VectorizerFactory:
    """
    Factory for managing text vectorizers.
    """
    _registry = {}
    logger = get_logger("VectorizerFactory")

    @classmethod
    def register_vectorizer(cls, name: str, vectorizer_cls):
        cls._registry[name] = vectorizer_cls
        cls.logger.info(f"Registered vectorizer: {name}")

    @classmethod
    def get_vectorizer(cls, name: str, **kwargs):
        cls.logger.info(f"Retrieving vectorizer class for name: {name}")
        vectorizer_cls = cls._registry.get(name)

        if not vectorizer_cls:
            cls.logger.warning(f"Vectorizer '{name}' not found in registry")
            return None

        # Convert ngram_range from list to tuple if needed
        if "ngram_range" in kwargs and isinstance(kwargs["ngram_range"], list):
            kwargs["ngram_range"] = tuple(kwargs["ngram_range"])

        cls.logger.info(f"Instantiating vectorizer '{name}' with kwargs: {kwargs}")
        return vectorizer_cls(name=name, **kwargs)
