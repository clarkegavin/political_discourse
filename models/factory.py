#models/factory.py
from logs.logger import get_logger
import traceback

class ModelFactory:
    """
    Factory for managing data models.
    """
    _registry = {}

    logger = get_logger("ModelFactory")

    @classmethod
    def register_model(cls, name: str, model):
        cls._registry[name] = model
        cls.logger.info(f"Registered model: {name}")

    @classmethod
    def get_model(cls, name: str, **kwargs):
        model = cls._registry.get(name)
        cls.logger.info(f"Retrieving model class for name: {name}")
        # cls.logger.info("ModelFactory call stack:\n" + "".join(traceback.format_stack(limit=10)))
        if not model:
            cls.logger.warning(f"Model '{name}' not found in registry")
        cls.logger.info(f"Instantiating model '{name}' with kwargs: {kwargs}")
        return model(name, **kwargs) if model else None