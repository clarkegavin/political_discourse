import os
from typing import Optional

from dotenv import load_dotenv
from openai import OpenAI

from llm.base import LLMClient


class OpenAIClient(LLMClient):
    """
    OpenAI implementation of the generic LLMClient interface.
    """

    def __init__(
        self,
        model: str,
        **kwargs
    ):
        super().__init__(
            model=model,
            **kwargs
        )

        load_dotenv()

        api_key = os.getenv("OPENAI_API_KEY")

        if not api_key:
            raise ValueError(
                "OPENAI_API_KEY environment variable is not configured"
            )

        self.client = OpenAI(
            api_key=api_key
        )

    def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        **kwargs
    ) -> str:

        input_content = []

        if system_prompt:
            input_content.append(
                {
                    "role": "system",
                    "content": system_prompt,
                }
            )

        input_content.append(
            {
                "role": "user",
                "content": prompt,
            }
        )

        response = self.client.responses.create(
            model=self.model,
            input=input_content,
            **kwargs
        )

        return response.output_text