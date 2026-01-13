#fetcher/factory.py
from .base import Fetcher

class FetcherFactory:
    """Factory for creating fetcher instances by name.

    This implements the factory method pattern: callers depend on the
    FetcherFactory abstraction to obtain fetchers (not the concrete classes).
    New fetchers can be registered without changing client code.
    """

    _registry = {}

    @classmethod
    def register(cls, name: str, fetcher_cls: type) -> None:
        """Register a new fetcher class under a friendly name."""
        cls._registry[name] = fetcher_cls

    @classmethod
    def create(cls, name: str, **kwargs) -> Fetcher:
        """Create a fetcher instance by name.

        Raises KeyError if name not registered.
        """
        fetcher_cls = cls._registry.get(name)
        if fetcher_cls is None:
            raise KeyError(f"Fetcher '{name}' is not registered. Available: {list(cls._registry.keys())}")
        return fetcher_cls(**kwargs)
