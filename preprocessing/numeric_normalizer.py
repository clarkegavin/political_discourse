# preprocessing/numeric_normalizer.py
from typing import List, Optional
import re
import pandas as pd
from .base import Preprocessor
from logs.logger import get_logger


class NumericNormalizer(Preprocessor):
    """
    Normalizes numeric expressions in text columns.

    Replacements performed (default):
    - money amounts: replaces patterns like '£100', '£ 100k', '£12m' -> 'money_amount'
    - percentages: replaces '12%' -> 'percentage'
    - years: replaces '1999' or '2020' -> 'year'

    Parameters
    - columns: optional list of column names to process; if None, all object/string columns are processed
    - strip: whether to trim whitespace after replacements
    """

    def __init__(self, columns: Optional[List[str]] = None, strip: bool = True):
        self.columns = columns
        self.strip = strip
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info(f"Initialized NumericNormalizer(columns={self.columns}, strip={self.strip})")

        # compiled regexes for speed
        self._re_money = re.compile(
            r'(?:£|€|\$)\s*\d+(?:[.,]\d+)?\s*[mbkMBK]?',
            flags=re.IGNORECASE
        )
        self._re_percentage = re.compile(r"\d+%")
        self._re_year = re.compile(r"\b(?:19|20)\d{2}\b")
        self._re_whitespace = re.compile(r"\s+")

    def fit(self, X):
        # stateless
        return self

    def _normalize_text(self, text: str) -> str:
        if not isinstance(text, str) or not text:
            return ""

        # Apply replacements
        # Money
        text = self._re_money.sub('[MONEY_AMOUNT]', text)
        # Percentages
        text = self._re_percentage.sub('[PERCENTAGE]', text)
        # Years
        text = self._re_year.sub('[YEAR]', text)

        # Normalize whitespace
        text = self._re_whitespace.sub(' ', text)
        if self.strip:
            text = text.strip()
        return text

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply numeric normalization to the configured columns (or all text columns).
        Returns a new DataFrame with replacements applied.
        """
        if not isinstance(df, pd.DataFrame):
            raise ValueError("NumericNormalizer.transform expects a pandas DataFrame")

        df = df.copy()

        target_columns = (self.columns if self.columns is not None
                          else df.select_dtypes(include=["object", "string"]).columns)

        self.logger.info(f"Applying NumericNormalizer to columns: {list(target_columns)}")

        for col in target_columns:
            if col not in df.columns:
                self.logger.debug(f"Column '{col}' not found in DataFrame; skipping")
                continue

            self.logger.debug(f"Normalizing numeric tokens in column: {col}")
            s = df[col].fillna("").astype(str)
            # apply normalization per-element
            s = s.map(self._normalize_text)
            df[col] = s

        self.logger.info("NumericNormalizer.transform completed")
        return df

    def get_params(self) -> dict:
        return {"columns": self.columns, "strip": self.strip}

