#preprocessing/remove_repeated_characters.py
import re
from typing import Any, Dict, Optional, List
import pandas as pd
from logs.logger import get_logger
from .base import Preprocessor


class RemoveRepeatedCharacters(Preprocessor):
    """Preprocessor that collapses runs of 3+ identical characters into two characters.

    Uses the regex r"(.)\1{2,}" -> r"\1\1".

    This class accepts an optional `columns` parameter (list of column names).
    If `columns` is not provided, the transformer will operate on all string/object
    columns in the DataFrame.

    Usage modes:
    - DataFrame mode: call `apply(df)` or `process(df)` to get a DataFrame back (used by pipelines).
    - Iterable/string mode: call `fit`/`transform`/`fit_transform` with an iterable of strings to get a list of cleaned strings.
    """

    logger = get_logger("RemoveRepeatedCharacters")

    def __init__(self,
                 columns: Optional[List[str]] = None,
                 pattern: str = r"(.)\1{2,}",
                 replace_with: str = ""):
        """Initialise the preprocessor.

        Parameters
        - columns: optional list of column names to process; if None, applies to all text/object columns
        - pattern: regex pattern to collapse repeated characters
        - replace_with: replacement string
        """
        self.columns = list(columns) if columns is not None else None
        self._pattern = re.compile(pattern)
        self.replace_with = replace_with
        self.logger.info(f"Initialized RemoveRepeatedCharacters(columns={self.columns}, pattern={pattern})")

    # Abstract-preprocessor compatibility -------------------------------------------------
    def fit(self, X):
        # Nothing to learn for this transformer
        return self

    def transform(self, X):
        self.logger.info("Transforming data with RemoveRepeatedCharacters")
        # DataFrame path ---------------------------------------------------------------
        if isinstance(X, pd.DataFrame):
            # determine target columns to operate on for sampling/logging
            if self.columns is None:
                target_columns = X.select_dtypes(include=["object", "string"]).columns.tolist()
            else:
                target_columns = [c for c in self.columns if c in X.columns]

            before_sample = {}

            # collect a small before-sample for any configured column that exists
            for col in target_columns:
                before_sample[col] = X[col].dropna().astype(str).head(3).tolist()

            df = self.apply(X)

            if before_sample:
                # build paired samples for the existing columns
                paired = []
                for col, sample in before_sample.items():
                    after_sample = df[col].dropna().astype(str).head(len(sample)).tolist() if col in df.columns else []
                    paired.append((col, list(zip(sample, after_sample))))
                safe_sample = str(paired).encode("ascii", "ignore").decode()
                self.logger.info(
                    f"[RemoveRepeatedCharacters] Sample before -> after per column: {safe_sample}"
                )

            return df

        # Series path: treat as iterable of strings and return cleaned list
        if isinstance(X, pd.Series):
            iterable = X

        # List / tuple path
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
        """Apply the repeated-character removal to the specified columns in the DataFrame.

        If `self.columns` is None, apply to all object/string dtype columns. If a
        provided column does not exist, it is skipped with a warning.
        Returns a new DataFrame with the columns processed.
        """
        df = df.copy()

        # Determine which columns to operate on
        if self.columns is None:
            target_columns = df.select_dtypes(include=["object", "string"]).columns.tolist()
        else:
            target_columns = self.columns

        if not target_columns:
            self.logger.warning("No target columns resolved for RemoveRepeatedCharacters; returning original DataFrame")
            return df

        for column in target_columns:
            if column not in df.columns:
                self.logger.warning(f"Column `{column}` not found in dataframe; skipping RemoveRepeatedCharacters for this column.")
                continue

            def _clean(val):
                if pd.isna(val):
                    return val
                try:
                    return self._pattern.sub(self.replace_with, str(val))
                except Exception as e:
                    self.logger.warning(f"Error cleaning value in `{column}`: {e}")
                    return val

            df[column] = df[column].apply(_clean)

        return df

    # alias used by some pipelines
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.apply(df)
