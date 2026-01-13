# preprocessing/factory.py
from typing import Type, Dict, Any
from .base import Preprocessor
from logs.logger import get_logger

class PreprocessorFactory:
    """Factory for creating preprocessors by name.

    Simple registry-based factory following the factory method pattern.
    """

    _registry: Dict[str, Type[Preprocessor]] = {}
    logger = get_logger("PreprocessorFactory")

    @classmethod
    def register(cls, name: str, preprocessor_cls: Type[Preprocessor]) -> None:
        cls._registry[name] = preprocessor_cls

    @classmethod
    def create(cls, name: str, **kwargs: Any) -> Preprocessor:
        cls.logger.info(f"Creating preprocessor '{name}' with params: {kwargs}")
        pre_cls = cls._registry.get(name)
        if pre_cls is None:
            raise KeyError(f"Preprocessor '{name}' not registered. Available: {list(cls._registry.keys())}")
        return pre_cls(**kwargs)

