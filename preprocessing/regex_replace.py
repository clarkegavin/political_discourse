import re
from typing import Iterable, List, Optional

import pandas as pd

from .base import Preprocessor
from logs.logger import get_logger


class RegexReplace(Preprocessor):
    """
    Generic regex replacement preprocessor.

    Can apply one or more regex replacement rules to one or more columns.

    Example YAML:

    preprocessors:
      - name: regex_replace
        params:
          columns: [DocumentText]
          replacements:
            - pattern: '^\\s*\\d+\\.\\s+'
              replacement: ''

            - pattern: '\\[\\d+/\\d+\\]'
              replacement: ''

            - pattern: '\\s*I propose to take Questions Nos?\\..*?together\\.\\s*'
              replacement: ' '
              ignore_case: true
              dotall: true
    """

    def __init__(
        self,
        columns: Optional[List[str]] = None,
        column: Optional[str] = None,
        replacements: Optional[List[dict]] = None,
    ):
        self.logger = get_logger(self.__class__.__name__)

        if column is not None and (columns is None or len(columns) == 0):
            columns = [column]

        self.columns = columns or []

        self.replacements = []

        for rule in replacements or []:

            flags = 0

            if rule.get("ignore_case", False):
                flags |= re.IGNORECASE

            if rule.get("multiline", False):
                flags |= re.MULTILINE

            if rule.get("dotall", False):
                flags |= re.DOTALL

            compiled = re.compile(rule["pattern"], flags)

            self.replacements.append(
                {
                    "pattern": compiled,
                    "replacement": rule.get("replacement", ""),
                }
            )

        self.logger.info(
            f"Initialized RegexReplace preprocessor "
            f"columns={self.columns}, rules={len(self.replacements)}"
        )

    def fit(self, X: Iterable[str]):
        # Stateless
        return self

    def _clean_text(self, value):

        if pd.isna(value):
            return value

        text = "" if value is None else str(value)

        for rule in self.replacements:
            text = rule["pattern"].sub(rule["replacement"], text)

        # Tidy whitespace that may have been introduced
        text = re.sub(r"\s{2,}", " ", text).strip()

        return text

    def transform(self, X):

        self.logger.info("Starting RegexReplace transformation")

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
                "Completed RegexReplace transformation on DataFrame"
            )

            return df

        # Iterable / Series path

        output = []

        for value in X:
            output.append(self._clean_text(value))

        self.logger.info("Completed RegexReplace transformation")

        return output

    def get_params(self) -> dict:

        return {
            "columns": self.columns,
            "replacement_count": len(self.replacements),
        }
