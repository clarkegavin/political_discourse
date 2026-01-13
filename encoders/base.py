from abc import ABC, abstractmethod
from typing import Any, Iterable, Optional


class Encoder(ABC):
    """Abstract encoder interface.

    Follow SOLID (especially the Interface Segregation and Dependency Inversion principles):
    - minimal, composable interface for encoders
    - code should depend on this abstraction, not concrete implementations
    """

    @abstractmethod
    def fit(self, y: Iterable[Any]) -> "Encoder":
        """Learn encoder mapping from y (labels).

        Returns self to allow chaining.
        """

    @abstractmethod
    def transform(self, y: Iterable[Any]) -> Iterable[Any]:
        """Transform labels to encoded representation."""

    def fit_transform(self, y: Iterable[Any]) -> Iterable[Any]:
        """Optional convenience: default implementation uses fit + transform."""
        self.fit(y)
        return self.transform(y)

    @abstractmethod
    def inverse_transform(self, y_enc: Iterable[Any]) -> Iterable[Any]:
        """Map encoded labels back to original representation."""


