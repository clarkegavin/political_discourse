# preprocessing/remove_urls.py
from typing import Iterable, List, Any, Optional
from .base import Preprocessor
from logs.logger import get_logger
import re
import pandas as pd

# optional pandas support
# try:
#     import importlib
#     pd = importlib.import_module('pandas')
# except Exception:
#     pd = None


class RemoveURLs(Preprocessor):
    """Preprocessor that removes URLs from a specified text field.

    Parameters
    ----------
    columns: str
        The list of field/columns to remove URLs from (required).
    pattern: str
        Regex pattern used to identify URLs. Default removes http(s) and www links.
    replace_with: str
        Replacement string for matched URLs (default: empty string).
    """

    def __init__(
        self,
        columns: Optional[List[str]] = None,
        pattern: str = r"https?://\S+|www\.\S+",
        replace_with: str = "",
    ):

        self.logger = get_logger(self.__class__.__name__)
        self.pattern = pattern
        self.replace_with = replace_with
        self.columns = columns

        try:
            self._compiled = re.compile(self.pattern, flags=re.IGNORECASE)
        except Exception:
            self._compiled = None
            self.logger.warning("Failed to compile URL regex; falling back to simple replace")

        self.logger.info(f"Initialized RemoveURLs columns={self.columns} pattern='{self.pattern}' replace_with='{self.replace_with}'")

    def fit(self, X: Iterable[Any]):
        # stateless
        return self

    # def _remove_from_text(self, text: Any) -> str:
    #     s = "" if text is None else str(text)
    #     try:
    #         if self._compiled:
    #             return self._compiled.sub(self.replace_with, s)
    #         return re.sub(self.pattern, self.replace_with, s, flags=re.IGNORECASE)
    #     except Exception:
    #         return s

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Starting RemoveURLs.transform - data type is %s", type(df))
        df = df.copy()

        target_columns = (
            self.columns
            if self.columns is not None
            else df.select_dtypes(include=["object", "string"]).columns
        )

        self.logger.info(f"Target columns for URL removal: {target_columns}")

        for col in target_columns:
            if col not in df.columns:
                self.logger.warning(f"Column '{col}' not found, skipping")
                continue

            self.logger.info(f"Removing URLs from column: {col}")

            if self._compiled:
                df[col] = df[col].apply(
                    lambda x: self._compiled.sub(self.replace_with, x)
                    if isinstance(x, str)
                    else x
                )
            else:
                df[col] = df[col].apply(
                    lambda x: re.sub(self.pattern, self.replace_with, x, flags=re.IGNORECASE)
                    if isinstance(x, str)
                    else x
                )

        return df

        #
        #
        # # If input is already a DataFrame
        # if isinstance(X, pd.DataFrame):
        #     self.logger.info(f"Input is a DataFrame with columns: {X.columns.tolist()}")
        #     df = X.copy()
        #     if self.field not in df.columns:
        #         self.logger.warning(f"Field '{self.field}' not present in DataFrame; returning original DataFrame")
        #         return df
        #     df[self.field] = df[self.field].astype(str).apply(self._remove_from_text)
        #     return df
        #
        # # If input is a list, dict-like iterable, or Series (applies_to='text')
        # out: List[Any] = []
        # for item in X:
        #     try:
        #         if isinstance(item, dict) and self.field in item:
        #             copy = dict(item)
        #             copy[self.field] = self._remove_from_text(copy.get(self.field, ""))
        #             out.append(copy)
        #         else:
        #             # fallback for plain strings
        #             out.append(self._remove_from_text(item))
        #     except Exception:
        #         out.append(item)
        #
        # # Wrap result as DataFrame
        # return pd.DataFrame({self.field: out})

    def get_params(self) -> dict:
        return {"columns": self.columns, "pattern": self.pattern, "replace_with": self.replace_with}

