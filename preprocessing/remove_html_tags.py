from .base import Preprocessor
from logs.logger import get_logger
import re
import pandas as pd
from typing import List, Optional


class RemoveHTMLTags(Preprocessor):
    """
    Removes HTML tags from specified DataFrame columns while preserving inner text.

    - Replaces <br> and <br/> with a configurable separator
    - Removes other HTML tags but keeps inner text
    """

    def __init__(
        self,
        columns: Optional[List[str]] = None,
        br_replace: str = ", ",
        strip: bool = True,
    ):
        self.columns = columns
        self.br_replace = br_replace
        self.strip = strip

        self.logger = get_logger(self.__class__.__name__)
        self.logger.info(
            f"Initialized RemoveHTMLTags(columns={self.columns}, br_replace='{self.br_replace}', strip={self.strip})"
        )

        # compiled regexes
        self._re_br = re.compile(r"<br\s*/?>", flags=re.IGNORECASE)
        self._re_tags = re.compile(r"<[^>]+>")

    def fit(self, X):
        return self

    def _clean_value(self, v):
        try:
            if pd.isna(v):
                return v

            if not isinstance(v, str):
                return v

            s = v

            # replace <br> first
            s = self._re_br.sub(self.br_replace, s)

            # remove all other tags
            s = self._re_tags.sub("", s)

            if self.strip:
                s = s.strip()

            return s

        except Exception as e:
            self.logger.warning(f"Failed to clean value: {e}")
            return v

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(df, pd.DataFrame):
            raise ValueError("RemoveHTMLTags.transform expects a pandas DataFrame")

        df = df.copy()

        # Correct default behaviour
        target_columns = (
            self.columns
            if self.columns is not None
            else df.select_dtypes(include=["object", "string"]).columns
        )

        self.logger.info(f"Applying RemoveHTMLTags to columns: {list(target_columns)}")

        for col in target_columns:
            if col not in df.columns:
                self.logger.warning(f"Column '{col}' not found, skipping")
                continue

            self.logger.info(f"Cleaning HTML tags in column: {col}")
            df[col] = df[col].apply(self._clean_value)

        self.logger.info("Completed RemoveHTMLTags.transform")
        return df

    def get_params(self) -> dict:
        return {
            "columns": self.columns,
            "br_replace": self.br_replace,
            "strip": self.strip,
        }