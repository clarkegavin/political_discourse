import re
from typing import Iterable, List, Optional

import pandas as pd

from .base import Preprocessor
from logs.logger import get_logger


class RemoveLeadingReferenceNumber(Preprocessor):
    """
    Removes a leading numeric reference from the beginning of a document.

    Examples:
        "192. Deputy Peter 'Chap' Cleere asked..."
            -> "Deputy Peter 'Chap' Cleere asked..."

        "4. Minister for Health..."
            -> "Minister for Health..."

    Only removes a number that appears at the very start of the text,
    followed by a period and one or more spaces.
    """

    def __init__(
        self,
        columns: Optional[List[str]] = None,
        column: Optional[str] = None,
    ):
        self.logger = get_logger(self.__class__.__name__)

        if column is not None and (columns is None or len(columns) == 0):
            columns = [column]

        self.columns = columns or []

        # Matches:
        #   1.
        #   23.
        #   192.
        # etc., only at the start of the string.
        self.pattern = re.compile(r"^\s*\d+\.\s+")

        self.logger.info(
            f"Initialized RemoveLeadingReferenceNumber preprocessor columns={self.columns}"
        )

    def fit(self, X: Iterable[str]):
        # Stateless
        return self

    def _clean_text(self, value):
        if pd.isna(value):
            return value

        text = "" if value is None else str(value)
        return self.pattern.sub("", text)

    def transform(self, X):
        self.logger.info("Starting RemoveLeadingReferenceNumber transformation")

        if isinstance(X, pd.DataFrame):
            if not self.columns:
                self.logger.warning(
                    "No columns configured; returning original DataFrame"
                )
                return X

            df = X.copy()

            for col in self.columns:
                if col not in df.columns:
                    self.logger.warning(
                        f"Column '{col}' not found; skipping"
                    )
                    continue

                try:
                    df[col] = df[col].apply(self._clean_text)
                except Exception as e:
                    self.logger.warning(
                        f"Failed processing column '{col}': {e}"
                    )

            self.logger.info(
                "Completed RemoveLeadingReferenceNumber transformation on DataFrame"
            )
            return df

        # Iterable path
        output = []

        for value in X:
            output.append(self._clean_text(value))

        self.logger.info(
            "Completed RemoveLeadingReferenceNumber transformation"
        )
        return output

    def get_params(self) -> dict:
        return {"columns": self.columns}