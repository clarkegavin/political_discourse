#scalers/factory.py
from .base import Scaler
from logs.logger import get_logger
from typing import Any, Dict

class ScalerFactory:
    """Factory for scalers. Register scaler classes and instantiate by name."""

    _registry: Dict[str, Any] = {}
    logger = get_logger("ScalerFactory")

    @classmethod
    def register(cls, name: str, scaler_cls: Any):
        cls._registry[name] = scaler_cls
        cls.logger.info(f"Registered scaler: {name}")

    @classmethod
    def get_scaler(cls, config: Dict[str, Any]):
        """Create scaler instance from config dict expected to contain 'name' and optional 'params'.

        Example config: {"name": "standard", "params": {"with_mean": True}}
        """
        if not config:
            return None

        name = config.get("name")
        if not name:
            raise ValueError("Scaler config must include 'name'")

        scaler_cls = cls._registry.get(name)
        if scaler_cls is None:
            raise KeyError(f"Scaler '{name}' is not registered. Available: {list(cls._registry.keys())}")

        params = config.get("params", {}) or {}
        cls.logger.info(f"Instantiating scaler '{name}' with params: {params}")
        return scaler_cls(**params)

    @classmethod
    def create_scaler(cls, name: str, **kwargs) -> Scaler:
        """Create scaler instance by name with optional kwargs."""
        scaler_cls = cls._registry.get(name)
        if not scaler_cls:
            raise KeyError(f"Scaler '{name}' is not registered. Available: {list(cls._registry.keys())}")

        cls.logger.info(f"Creating scaler '{name}' with kwargs: {kwargs}")
        return scaler_cls(**kwargs)
