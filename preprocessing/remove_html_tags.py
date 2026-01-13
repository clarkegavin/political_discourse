# preprocessing/remove_html_tags.py
from .base import Preprocessor
from logs.logger import get_logger
import re
import pandas as pd
from typing import List, Optional

class RemoveHTMLTags(Preprocessor):
    """
    RemoveHTMLTags preprocessor

    Removes HTML tags from specified DataFrame columns while preserving inner text.
    - Replaces <br> and <br/> with a configurable separator (default: ', ').
    - Removes other tags (e.g. <strong>, </strong>) but preserves the inner text.

    Parameters:
    - columns: List[str] columns to clean
    - br_replace: str replacement for <br> tags (default: ', ')
    - strip: bool strip whitespace around results (default True)
    """

    def __init__(self, columns: Optional[List[str]] = None, br_replace: str = ', ', strip: bool = True):
        self.columns = columns or []
        self.br_replace = br_replace
        self.strip = strip
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info(f"Initialized RemoveHTMLTags columns={self.columns} br_replace='{self.br_replace}' strip={self.strip}")

        # compiled regexes
        # replace <br> or <br/> (case-insensitive) with br_replace
        self._re_br = re.compile(r"<br\s*/?>", flags=re.IGNORECASE)
        # remove any other tags like <strong>, </strong>, <a href=...>, etc.
        self._re_tags = re.compile(r"<[^>]+>")

    def fit(self, X):
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Starting RemoveHTMLTags.transform")
        if not isinstance(X, pd.DataFrame):
            self.logger.error("RemoveHTMLTags.transform expects a pandas DataFrame")
            raise ValueError("RemoveHTMLTags.transform expects a pandas DataFrame")

        df = X.copy()

        for col in self.columns:
            if col not in df.columns:
                self.logger.info(f"Column '{col}' not found in DataFrame, skipping")
                continue

            self.logger.info(f"Cleaning HTML tags in column: {col}")

            def _clean_value(v):
                try:
                    if pd.isna(v):
                        return v
                    s = str(v)
                    # replace br tags with separator first
                    s = self._re_br.sub(self.br_replace, s)
                    # remove remaining tags but keep inner text
                    s = self._re_tags.sub('', s)
                    if self.strip:
                        s = s.strip()
                    return s
                except Exception as e:
                    self.logger.warning(f"Failed to clean cell value in column '{col}': {e}")
                    return v

            df[col] = df[col].apply(_clean_value)

        self.logger.info("Completed RemoveHTMLTags.transform")
        return df

    def get_params(self) -> dict:
        return {"columns": self.columns, "br_replace": self.br_replace, "strip": self.strip}

