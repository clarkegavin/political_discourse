#preprocessing/remove_repeated_characters.py
import re
from typing import Any, Dict, Optional
import pandas as pd
from logs.logger import get_logger
from .base import Preprocessor


class RemoveRepeatedCharacters(Preprocessor):
    """Preprocessor that collapses runs of 3+ identical characters into two characters.

    Uses the regex r"(.)\1{2,}" -> r"\1\1".
    Expects params to be a dict with key "field" pointing to the column to process.

    This class provides two usage modes:
    - DataFrame mode: call `apply(df)` or `process(df)` to get a DataFrame back (used by pipelines).
    - Iterable/string mode: call `fit`/`transform`/`fit_transform` with an iterable of strings to get a list of cleaned strings.
    """

    logger = get_logger("RemoveRepeatedCharacters")

    #def __init__(self, params: Optional[Dict[str, Any]] = None):
    def __init__(self, field: str
                 , pattern: str = r"(.)\1{2,}"
                 , replace_with: str=''):
        #params = params or {}
        self.field = field
        if not self.field:
            raise ValueError("`field` must be provided in params for RemoveRepeatedCharacters")
        self._pattern = re.compile(pattern)
        self.replace_with = replace_with
        self.logger.info(f"Initialized RemoveRepeatedCharacters(field={self.field}, pattern={pattern})")

    # Abstract-preprocessor compatibility -------------------------------------------------
    def fit(self, X):
        # Nothing to learn for this transformer
        return self

    def transform(self, X):
        self.logger.info("Transforming data with RemoveRepeatedCharacters")
        if isinstance(X, pd.DataFrame):
            before_sample = None

            if self.field in X.columns:
                # grab a tiny sample before cleaning
                before_sample = X[self.field].dropna().astype(str).head(3).tolist()

            df = self.apply(X)

            if before_sample:
                after_sample = df[self.field].dropna().astype(str).head(3).tolist()
                safe_sample = str(list(zip(before_sample, after_sample))).encode("ascii", "ignore").decode()
                self.logger.info(
                    f"[RemoveRepeatedCharacters] Sample before -> after: "
                    f"{safe_sample}"
                )

            return df

            # --- Case 2: Series ---
        if isinstance(X, pd.Series):
            iterable = X

            # --- Case 3: List / tuple ---
        elif isinstance(X, (list, tuple)):
            iterable = X

        else:
            raise TypeError(
                "RemoveRepeatedCharacters.transform expects a pandas.DataFrame, pandas.Series or iterable of strings"
            )

        def _clean_val(val):
            if pd.isna(val):
                return val
            try:
                return self._pattern.sub(self.replace_with, str(val))
            except Exception:
                return val

        self.logger.info("Transforming iterable data with RemoveRepeatedCharacters")
        return [_clean_val(v) for v in iterable]

    def fit_transform(self, X):
        self.fit(X)
        return self.transform(X)

    # DataFrame-oriented API used by pipelines -------------------------------------------
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply the repeated-character removal to the specified field in the DataFrame.

        Returns a new DataFrame with the field processed. If the field does not exist, the
        original DataFrame is returned unchanged.
        """
        if self.field not in df.columns:
            self.logger.warning(f"Field `{self.field}` not found in dataframe; skipping RemoveRepeatedCharacters.")
            return df

        df = df.copy()

        def _clean(val):
            if pd.isna(val):
                return val
            try:
                return self._pattern.sub(self.replace_with, str(val))
            except Exception as e:
                self.logger.warning(f"Error cleaning value in `{self.field}`: {e}")
                return val

        df[self.field] = df[self.field].apply(_clean)
        return df

    # alias used by some pipelines
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.apply(df)
