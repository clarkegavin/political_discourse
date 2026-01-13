import re
import pandas as pd
from logs.logger import get_logger
from .base import Preprocessor


class RemovePunctuationNoise(Preprocessor):
    """
    Preprocessor that collapses runs of repeated punctuation.

    Default:
        pattern: r"([!?.,])\\1+"
        replace_with: r"\\1"

    Works with:
    - DataFrame (in transform/apply)
    - pandas Series
    - list/tuple of strings
    """

    logger = get_logger("RemovePunctuationNoise")

    def __init__(
        self,
        field: str,
        pattern: str = r"([!?.,])\1+",
        replace_with: str = r"\1"
    ):
        self.field = field
        if not self.field:
            raise ValueError("`field` must be provided")

        self._pattern = re.compile(pattern)
        self.replace_with = replace_with

    # -------------------------------------------------------------------------
    def fit(self, X):
        return self

    def transform(self, X):
        self.logger.info("Transforming data with RemovePunctuationNoise")
        # Now supports DataFrame, Series, and iterables
        if isinstance(X, pd.DataFrame):
            return self.apply(X)
        elif isinstance(X, pd.Series):
            iterable = X
        elif isinstance(X, (list, tuple)):
            iterable = X
        else:
            raise TypeError(
                "RemovePunctuationNoise.transform expects a DataFrame, Series, or iterable of strings"
            )

        return [self._clean_value(v) for v in iterable]

    def fit_transform(self, X):
        return self.transform(X)

    # -------------------------------------------------------------------------
    def _clean_value(self, val):
        if pd.isna(val):
            return val
        try:
            return self._pattern.sub(self.replace_with, str(val))
        except Exception:
            return val

    # -------------------------------------------------------------------------
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.field not in df.columns:
            self.logger.warning(
                f"Field `{self.field}` not found in dataframe; skipping RemovePunctuationNoise."
            )
            return df

        df = df.copy()
        df[self.field] = df[self.field].apply(self._clean_value)
        return df

    # Alias for pipelines
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.apply(df)
