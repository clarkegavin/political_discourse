from .base import RepresentationModel
from logs.logger import get_logger

class RepresentationModelFactory:
    """Factory for representation models. Register model classes and instantiate by name."""

    _registry = {}
    logger = get_logger("RepresentationModelFactory")

    @classmethod
    def register(cls, name: str, model_cls):
        if name in cls._registry:
            return
        cls._registry[name] = model_cls
        cls.logger.info(f"Registered representation model: {name}")

    @classmethod
    def create_representation_model(cls, key: str, **kwargs):
        """
        Instantiate an experiment from the registry with kwargs.
        """
        representation_model = cls._registry.get(key)
        cls.logger.info(f"Retrieving representation model class for key: {key}")
        if not representation_model:
            cls.logger.warning(f"Representation model '{key}' not found in registry")
            return None
        return representation_model(**kwargs)