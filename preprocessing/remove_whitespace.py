# preprocessing/remove_whitespace.py
import re
from typing import Any, Dict, Optional
import pandas as pd
from logs.logger import get_logger
from .base import Preprocessor


class RemoveWhitespace(Preprocessor):
    """
    Collapses runs of whitespace into a single space and strips ends.
    Default pattern: r"\s+" -> replace_with " " (single space).

    Constructor signature matches RemoveRepeatedCharacters:
      def __init__(self, field: str, pattern: str = r"\s+", replace_with: str = " ")
    """

    logger = get_logger("RemoveWhitespace")

    def __init__(
        self,
        field: str,
        pattern: str = r"\s+",
        replace_with: str = " "
    ):
        self.field = field
        if not self.field:
            raise ValueError("`field` must be provided in params for RemoveWhitespace")

        self._pattern = re.compile(pattern)
        self.replace_with = replace_with

    # Abstract-preprocessor compatibility -------------------------------------------------
    def fit(self, X):
        # Nothing to learn
        return self

    def transform(self, X):
        """
        Accepts:
         - pd.DataFrame -> delegates to apply() and returns DataFrame
         - pd.Series -> returns list of cleaned values (same behaviour as other preprocessors)
         - list/tuple -> returns list of cleaned values
        """
        # --- DataFrame: delegate to apply() for consistency with pipeline expectations
        if isinstance(X, pd.DataFrame):
            return self.apply(X)

        # --- Series or iterable
        if isinstance(X, pd.Series):
            iterable = X
        elif isinstance(X, (list, tuple)):
            iterable = X
        else:
            raise TypeError(
                "RemoveWhitespace.transform expects a pandas.DataFrame, pandas.Series or an iterable of strings"
            )

        def _clean(val):
            if pd.isna(val):
                return val
            try:
                return self._pattern.sub(self.replace_with, str(val)).strip()
            except Exception:
                return val

        cleaned = [_clean(v) for v in iterable]

        # log a small sample for visibility (sanitise to avoid encoding errors)
        try:
            sample_before = list(map(lambda v: ("" if pd.isna(v) else str(v)) , iterable[:3]))
            sample_after = cleaned[:3]
            sample_pairs = list(zip(sample_before, sample_after))
            safe_sample = str(sample_pairs).encode("ascii", "ignore").decode()
            self.logger.info(f"[RemoveWhitespace] Sample before -> after: {safe_sample}")
        except Exception:
            # Never fail the transform because logging failed
            pass

        return cleaned

    def fit_transform(self, X):
        self.fit(X)
        return self.transform(X)

    # DataFrame-oriented API used by pipelines -------------------------------------------
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.field not in df.columns:
            self.logger.warning(f"[RemoveWhitespace] Field `{self.field}` not found in dataframe; skipping.")
            return df

        df = df.copy()

        # sample before for logging
        try:
            before_sample = df[self.field].dropna().astype(str).head(3).tolist()
        except Exception:
            before_sample = []

        def _clean(val):
            if pd.isna(val):
                return val
            try:
                return self._pattern.sub(self.replace_with, str(val)).strip()
            except Exception as e:
                self.logger.warning(f"[RemoveWhitespace] Error cleaning value in `{self.field}`: {e}")
                return val

        df[self.field] = df[self.field].apply(_clean)

        # sample after and log sanitised sample
        try:
            after_sample = df[self.field].dropna().astype(str).head(3).tolist()
            sample_pairs = list(zip(before_sample, after_sample))
            safe_sample = str(sample_pairs).encode("ascii", "ignore").decode()
            self.logger.info(f"[RemoveWhitespace] Sample before -> after: {safe_sample}")
        except Exception:
            pass

        return df

    # alias used by some pipelines
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.apply(df)
