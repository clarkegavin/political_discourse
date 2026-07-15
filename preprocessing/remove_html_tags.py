#preprocessing/remove_html_tags.py
import re
import html
import pandas as pd
from typing import List, Optional
from bs4 import BeautifulSoup

from .base import Preprocessor
from logs.logger import get_logger


class RemoveHTMLTags(Preprocessor):
    """
    Aggressive cleaning for boards.ie style posts.
    - Completely removes all js-embed divs and spans (quotes, PDFs, links)
    - Keeps only the actual main comment text
    """

    def __init__(self, columns: Optional[List[str]] = None, strip: bool = True):
        self.columns = columns
        self.strip = strip
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info(f"Initialized RemoveHTMLTags(columns={self.columns}, strip={self.strip})")

        self._re_whitespace = re.compile(r"\s+")

    def fit(self, X):
        # Stateless transformer
        return self

    def _remove_embed_blocks(self, text: str) -> str:
        """Remove entire embed divs and spans (most important step)"""
        if not text:
            return text

        # 1. Remove entire <div class="js-embed ..."> ... </div> (handles malformed)
        text = re.sub(r'<div[^>]*class="[^"]*js-embed[^"]*"[^>]*>.*?</div>',
                      ' ', text, flags=re.DOTALL | re.IGNORECASE)

        # 2. Remove entire <span class="js-embed ..."> ... </span>
        text = re.sub(r'<span[^>]*class="[^"]*js-embed[^"]*"[^>]*>.*?</span>',
                      ' ', text, flags=re.DOTALL | re.IGNORECASE)

        # 3. Safety: remove any remaining data-embedjson blocks
        text = re.sub(r'data-embedjson="[^"]*"', ' ', text, flags=re.DOTALL)

        return text

    def _clean_text(self, text: str) -> str:
        """Full cleaning pipeline"""
        if not isinstance(text, str) or not text.strip():
            return ""

        # Decode HTML entities first
        text = html.unescape(text)

        # Remove entire embed blocks (critical)
        text = self._remove_embed_blocks(text)

        # Remove scripts, styles
        text = re.sub(r'<script.*?</script>|<style.*?</style>', ' ', text, flags=re.DOTALL | re.IGNORECASE)

        # Remove all <a> tags and their content
        text = re.sub(r'<a[^>]*>.*?</a>', ' ', text, flags=re.DOTALL | re.IGNORECASE)

        # Remove all remaining HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)

        # Remove PDF binary garbage (just in case anything leaked)
        text = re.sub(r'%PDF-.*?(?:endobj|endstream)', ' ', text, flags=re.DOTALL)
        text = re.sub(r'\d+\s+0\s+obj', ' ', text)

        # Normalize whitespace
        text = self._re_whitespace.sub(' ', text)
        text = re.sub(r'\s+([.,!?])', r'\1', text)  # fix punctuation spacing

        return text.strip()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(df, pd.DataFrame):
            raise ValueError("RemoveHTMLTags.transform expects a pandas DataFrame")

        df = df.copy()
        target_columns = (self.columns if self.columns is not None
                          else df.select_dtypes(include=["object", "string"]).columns)

        self.logger.info(f"Applying RemoveHTMLTags to columns: {list(target_columns)}")

        for col in target_columns:
            if col not in df.columns:
                continue

            self.logger.info(f"Cleaning column: {col}")

            s = df[col].fillna("").astype(str)
            s = s.str.strip()  # optional: clean whitespace early

            # Apply cleaning
            s = s.map(self._clean_text)

            if self.strip:
                s = s.str.strip()

            df[col] = s

        self.logger.info("RemoveHTMLTags.transform completed")
        return df

    def get_params(self) -> dict:
        return {"columns": self.columns, "strip": self.strip}