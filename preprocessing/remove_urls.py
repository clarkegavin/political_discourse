#preprocessing/remove_urls.py
import re
import pandas as pd
from typing import Optional, List, Any
from .base import Preprocessor
from logs.logger import get_logger


class RemoveURLs(Preprocessor):
    """
    Removes URLs from text reliably.
    Handles both clean URLs and messy social media links (Facebook, etc.).
    """

    def __init__(
            self,
            columns: Optional[List[str]] = None,
            replace_with: str = "",
    ):
        self.logger = get_logger(self.__class__.__name__)
        self.columns = columns
        self.replace_with = replace_with

        # Improved regex: catches normal URLs + long query strings
        #self.pattern = r'https?://\S+'
        self.pattern = r'(?i)\b(?:https?://|www\.)[^\s<>"\']+'

        self._compiled = re.compile(self.pattern, flags=re.IGNORECASE)

        self.logger.info(f"Initialized RemoveURLs(columns={columns})")

    def fit(self, X=None):
        return self

    def _remove_urls_from_text(self, text: Any) -> str:
        if text is None or not isinstance(text, str):
            return "" if text is None else str(text)

        # Step 1: Remove all URLs
        cleaned = self._compiled.sub(self.replace_with, text)

        # Step 2: Extra cleanup for broken/messy Facebook-style leftovers
        # This catches cft[0]=..., tn=..., etc. that sometimes remain
        cleaned = re.sub(r'\b(cft|tn)\s*\[?\d*\]?\s*=\s*[^ ]+', '', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'\b[cfttn][^ ]*=[^ ]+', '', cleaned, flags=re.IGNORECASE)

        # Final cleanup
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()

        return cleaned

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(df, pd.DataFrame):
            raise ValueError("RemoveURLs.transform expects a pandas DataFrame")

        df = df.copy()

        target_columns = (
            self.columns
            if self.columns is not None
            else df.select_dtypes(include=["object", "string"]).columns
        )

        self.logger.info(f"Removing URLs from columns: {list(target_columns)}")

        for col in target_columns:
            if col not in df.columns:
                continue
            df[col] = df[col].apply(self._remove_urls_from_text)

        return df