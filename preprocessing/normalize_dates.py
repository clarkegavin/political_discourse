# preprocessing/normalize_dates.py
from typing import List, Optional
import re
import pandas as pd
from .base import Preprocessor
from logs.logger import get_logger


class DateNormalizer(Preprocessor):
    """
    Normalizes date expressions in text columns.

    Replacements performed:
    - full/explicit dates -> [DATE]
    - standalone years (e.g., 1999, 2020) -> [YEAR]

    Parameters
    - columns: optional list of column names to process; if None, all object/string columns are processed
    - token_date: replacement token for dates
    - token_year: replacement token for standalone years
    - strip: whether to trim whitespace after replacements
    """

    def __init__(self, columns: Optional[List[str]] = None, token_date: str = "[DATE]", token_year: str = "[YEAR]", strip: bool = True):
        self.columns = list(columns) if columns is not None else None
        self.token_date = token_date
        self.token_year = token_year
        self.strip = strip
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info(f"Initialized DateNormalizer(columns={self.columns}, token_date={self.token_date}, token_year={self.token_year})")

        # compile regexes
        self._re_date = re.compile(
            r"\b(?:"
            r"\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]?\d{2,4}"                      # numeric dates
            r"|\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*"  # 13 June
            r"(?:\s+\d{2,4})?"                                                      # optional year
            r"|\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{1,2}"
            r")\b",
            flags=re.IGNORECASE
        )

        self._re_year = re.compile(r"\b(?:19|20)\d{2}\b", flags=re.IGNORECASE)
        self._re_whitespace = re.compile(r"\s+")

    def fit(self, X):
        # stateless
        return self

    def _normalize_text(self, text: str) -> str:
        if not isinstance(text, str) or not text:
            return ""

        # First replace explicit/complex dates
        try:
            text = self._re_date.sub(self.token_date, text)
        except Exception:
            # on regex failure, return original text
            return text

        # Then replace standalone years
        try:
            text = self._re_year.sub(self.token_year, text)
        except Exception:
            return text

        # Normalize whitespace
        text = self._re_whitespace.sub(' ', text)
        if self.strip:
            text = text.strip()
        return text

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply date normalization to the configured columns (or all text columns).
        Returns a new DataFrame with replacements applied.
        """
        if not isinstance(df, pd.DataFrame):
            # support iterable/series interface similar to other preprocessors
            if isinstance(df, pd.Series) or isinstance(df, (list, tuple)):
                iterable = df
                out = []
                for v in iterable:
                    if pd.isna(v):
                        out.append(v)
                    else:
                        out.append(self._normalize_text(str(v)))
                return out

            raise ValueError("DateNormalizer.transform expects a pandas DataFrame or iterable/Series of strings")

        df = df.copy()

        target_columns = (self.columns if self.columns is not None
                          else df.select_dtypes(include=["object", "string"]).columns.tolist())

        self.logger.info(f"Applying DateNormalizer to columns: {target_columns}")

        for col in target_columns:
            if col not in df.columns:
                self.logger.debug(f"Column '{col}' not found in DataFrame; skipping")
                continue

            s = df[col].fillna("").astype(str)
            s = s.map(self._normalize_text)
            df[col] = s

        self.logger.info("DateNormalizer.transform completed")
        return df

    def get_params(self) -> dict:
        return {"columns": self.columns, "token_date": self.token_date, "token_year": self.token_year, "strip": self.strip}

