# preprocessing/lowercase.py
from typing import Iterable, List
from .base import Preprocessor
from logs.logger import get_logger


class Lowercase(Preprocessor):
    """Simple preprocessor that lowercases text.

    Behaviour:
    - Accepts iterables or pandas Series (stringified prior to this call by pipeline).
    - Returns a list of lowercased strings.
    """

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("Initialized Lowercase preprocessor")
        self.lower_case = True

    def fit(self, X: Iterable[str]):
        # stateless
        return self

    def transform(self, X: Iterable[str]) -> List[str]:
        self.logger.info("Starting Lowercase transformation")
        out: List[str] = []
        for i, v in enumerate(X):
            try:
                s = "" if v is None else str(v)
                out.append(s.lower())
            except Exception as e:
                self.logger.warning(f"Lowercase transform failed for index {i}: {e}; using original value")
                out.append(str(v))
        self.logger.info("Completed Lowercase transformation")
        return out

    def get_params(self) -> dict:
        return {'lower_case': self.lower_case}
