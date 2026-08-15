from typing import Any, Dict

from llm.base import LLMClient
from llm.openai_client import OpenAIClient


class LLMFactory:
    """
    Factory for creating provider-specific LLM clients.
    """

    _providers = {
        "openai": OpenAIClient,
    }

    @classmethod
    def create(
        cls,
        provider: str,
        model: str,
        **kwargs
    ) -> LLMClient:

        provider = provider.lower()

        client_class = cls._providers.get(provider)

        if client_class is None:
            supported = ", ".join(
                sorted(cls._providers.keys())
            )

            raise ValueError(
                f"Unsupported LLM provider '{provider}'. "
                f"Supported providers: {supported}"
            )

        return client_class(
            model=model,
            **kwargs
        )