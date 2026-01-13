# imputers/factory.py
from typing import Type, Dict, Any
from .base import Imputer
from logs.logger import get_logger


class ImputerFactory:
    """Registry factory for imputers."""

    _registry: Dict[str, Type[Imputer]] = {}
    logger = get_logger("ImputerFactory")

    @classmethod
    def register(cls, name: str, imputer_cls: Type[Imputer]) -> None:
        cls._registry[name] = imputer_cls

    @classmethod
    def create(cls, name: str, **kwargs: Any) -> Imputer:
        cls.logger.info(f"Creating imputer '{name}' with params: {kwargs}")
        imputer_cls = cls._registry.get(name)
        if imputer_cls is None:
            raise KeyError(f"Imputer '{name}' not registered. Available: {list(cls._registry.keys())}")
        return imputer_cls(**kwargs)

