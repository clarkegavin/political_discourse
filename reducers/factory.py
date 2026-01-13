# reducers/factory.py
from typing import Any, Dict
from logs.logger import get_logger

from .base import Reducer


class ReducerFactory:
    """Factory for reducers. Register reducer classes and instantiate by name."""

    # use Any for reducer classes to avoid static type complaints when calling with kwargs
    _registry: Dict[str, Any] = {}
    logger = get_logger("ReducerFactory")

    @classmethod
    def register(cls, name: str, reducer_cls: Any):
        cls._registry[name] = reducer_cls
        cls.logger.info(f"Registered reducer: {name}")

    @classmethod
    def get_reducer(cls, config: Dict[str, Any]):
        """Create reducer instance from config dict expected to contain 'name' and optional 'params'.

        Example config: {"name": "umap", "params": {"n_components": 2}}
        """
        if not config:
            return None

        name = config.get("name")
        if not name:
            raise ValueError("Reducer config must include 'name'")

        reducer_cls = cls._registry.get(name)
        if reducer_cls is None:
            raise KeyError(f"Reducer '{name}' is not registered. Available: {list(cls._registry.keys())}")

        params = config.get("params", {}) or {}
        cls.logger.info(f"Instantiating reducer '{name}' with params: {params}")
        return reducer_cls(name=name, **params)

    @classmethod
    def create_reducer(cls, name: str, **kwargs) -> Reducer:
        """Create reducer instance by name with optional kwargs."""
        reducer_cls = cls._registry.get(name)
        if not reducer_cls:
            raise KeyError(f"Reducer '{name}' is not registered. Available: {list(cls._registry.keys())}")

        cls.logger.info(f"Creating reducer '{name}' with kwargs: {kwargs}")
        return reducer_cls(**kwargs)

    @classmethod
    def get_reducers(cls, config):
        """
        Create a list of reducers from config.
        Accepts:
          - None
          - single reducer dict
          - list of reducer dicts
        """
        if not config:
            return []

        # normalize to list
        if isinstance(config, dict):
            config = [config]

        if not isinstance(config, list):
            raise TypeError(
                f"Reducer config must be dict or list of dicts, got {type(config)}"
            )

        reducers = []
        for cfg in config:
            reducer = cls.get_reducer(cfg)
            reducers.append(reducer)

        return reducers