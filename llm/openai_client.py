import os
import time
from typing import Optional

from dotenv import load_dotenv
from openai import OpenAI
from logs.logger import get_logger
from llm.base import LLMClient


class OpenAIClient(LLMClient):
    """
    OpenAI implementation of the generic LLMClient interface.
    """

    def __init__(
        self,
        model: str,
        max_retries: int = 5,
        retry_delay: float = 2.0,
        **kwargs
    ):
        super().__init__(
            model=model,
            **kwargs
        )
        self.logger = get_logger(
            self.__class__.__name__
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

        self.max_retries = max_retries
        self.retry_delay = retry_delay

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

        for attempt in range(
            self.max_retries + 1
        ):

            try:

                response = self.client.responses.create(
                    model=self.model,
                    input=input_content,
                    **kwargs
                )

                return response.output_text

            except Exception as e:

                # -------------------------------------------------
                # Determine whether this looks like a transient
                # API failure that is worth retrying.
                # -------------------------------------------------

                status_code = getattr(
                    e,
                    "status_code",
                    None
                )

                retryable = (
                    status_code in {
                        429,
                        500,
                        502,
                        503,
                        504,
                    }
                )

                # -------------------------------------------------
                # Non-retryable error
                # -------------------------------------------------

                if not retryable:

                    raise

                # -------------------------------------------------
                # Retries exhausted
                # -------------------------------------------------

                if attempt >= self.max_retries:

                    raise

                # -------------------------------------------------
                # Exponential backoff
                # -------------------------------------------------

                delay = self.retry_delay * (
                    2 ** attempt
                )

                self.logger.warning(
                    f"OpenAI request failed "
                    f"(status={status_code}). "
                    f"Retrying in {delay:.1f}s "
                    f"(attempt {attempt + 1}/"
                    f"{self.max_retries})..."
                )

                time.sleep(delay)

        raise RuntimeError(
            "OpenAI request failed unexpectedly"
        )