from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class LLMClient(ABC):
    """
    Provider-independent interface for LLM clients.

    Topic modelling code should interact only with this interface
    and should not contain provider-specific API logic.
    """

    def __init__(
        self,
        model: str,
        **kwargs
    ):
        self.model = model

    @abstractmethod
    def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate a text response from the LLM.

        Parameters
        ----------
        prompt:
            User/input prompt.

        system_prompt:
            Optional system-level instruction.

        Returns
        -------
        str
            Generated text response.
        """
        raise NotImplementedError