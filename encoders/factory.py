from typing import Dict, Type, Any

from .base import Encoder
from .label_encoder import SklearnLabelEncoder


class EncoderFactory:
    """Factory for creating encoder instances by name.

    This implements the factory method pattern: callers depend on the
    EncoderFactory abstraction to obtain encoders (not the concrete classes).
    New encoders can be registered without changing client code.
    """

    # _registry: Dict[str, Type[Encoder]] = {
    #     "label": SklearnLabelEncoder,
    #     "sklearn_label": SklearnLabelEncoder,
    # }
    _registry = {}

    @classmethod
    def register(cls, name: str, encoder_cls: Type[Encoder]) -> None:
        """Register a new encoder class under a friendly name."""
        cls._registry[name] = encoder_cls

    @classmethod
    def create(cls, name: str, **kwargs: Any) -> Encoder:
        """Create an encoder instance by name.

        Raises KeyError if name not registered.
        """
        encoder_cls = cls._registry.get(name)
        if encoder_cls is None:
            raise KeyError(f"Encoder '{name}' is not registered. Available: {list(cls._registry.keys())}")
        return encoder_cls(**kwargs)


