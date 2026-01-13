#sampler/factory.py
from logs.logger import get_logger

class SamplerFactory:
    """
    Factory for managing data samplers.
    """
    _registry = {}
    logger = get_logger("SamplerFactory")

    @classmethod
    def register_sampler(cls, name: str, sampler_cls):
        cls._registry[name] = sampler_cls
        cls.logger.info(f"Registered sampler: {name}")

    @classmethod
    def get_sampler(cls, name: str, **kwargs):
        cls.logger.info(f"Retrieving sampler class for name: {name}")
        sampler_cls = cls._registry.get(name)

        if not sampler_cls:
            cls.logger.warning(f"Sampler '{name}' not found in registry")
            return None

        cls.logger.info(f"Instantiating sampler '{name}' with kwargs: {kwargs}")
        return sampler_cls(name=name, **kwargs)