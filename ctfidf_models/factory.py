# ctfidf_models/factory.py
from logs.logger import get_logger

class CTFIDFModelFactory:
    """
    Factory for managing c-TF-IDF models (wrappers around bertopic.vectorizers.ClassTfidfTransformer).
    """
    _registry = {}
    logger = get_logger("CTFIDFModelFactory")

    @classmethod
    def register_ctfidf_model(cls, name: str, model_cls):
        if name in cls._registry:
            return
        cls._registry[name] = model_cls
        cls.logger.info(f"Registered c-TF-IDF model: {name}")

    @classmethod
    def get_ctfidf_model(cls, name: str, **kwargs):
        cls.logger.info(f"Retrieving c-TF-IDF model class for name: {name}")
        model_cls = cls._registry.get(name)

        if not model_cls:
            cls.logger.warning(f"c-TF-IDF model '{name}' not found in registry")
            return None

        # Convert list ngram_range to tuple if passed
        if "ngram_range" in kwargs and isinstance(kwargs["ngram_range"], list):
            kwargs["ngram_range"] = tuple(kwargs["ngram_range"])

        cls.logger.info(f"Instantiating c-TF-IDF model '{name}' with kwargs: {kwargs}")
        return model_cls(name=name, **kwargs)

