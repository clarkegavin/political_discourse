from .base import Preprocessor
from logs.logger import get_logger
import re
import pandas as pd
from typing import List, Optional
import html
from bs4 import BeautifulSoup

class RemoveHTMLTags(Preprocessor):
    """
    Removes HTML tags and embedded noise from text columns.

    Key features:
    - Decodes HTML entities
    - Fixes malformed HTML (e.g. <\/p>)
    - Removes scripts, styles, and embedded content (div/span js-embed)
    - Removes links entirely
    - Extracts clean text via BeautifulSoup
    - Performs final cleanup of residual artifacts
    """

    def __init__(
        self,
        columns: Optional[List[str]] = None,
        strip: bool = True,
    ):
        self.columns = columns
        self.strip = strip

        self.logger = get_logger(self.__class__.__name__)
        self.logger.info(
            f"Initialized RemoveHTMLTags(columns={self.columns}, strip={self.strip})"
        )

        # Precompiled regex patterns for performance
        self._re_script = re.compile(r"<script.*?>.*?</script>", flags=re.DOTALL | re.IGNORECASE)
        self._re_style = re.compile(r"<style.*?>.*?</style>", flags=re.DOTALL | re.IGNORECASE)

        # Remove div or span blocks with class containing "js-embed"
        self._re_embed_block = re.compile(
            r'<(div|span)[^>]*class="[^"]*js-embed[^"]*"[^>]*>.*?</\1>',
            flags=re.DOTALL | re.IGNORECASE
        )

        # Remove data-embedjson attributes
        self._re_data_embed = re.compile(
            r'data-embedjson=".*?"(?=\s|>)',
            flags=re.DOTALL | re.IGNORECASE
        )

        # Fix leftover HTML tags
        self._re_escaped_tags = re.compile(r"<\\/?[a-zA-Z]+>")
        self._re_leftover_tags = re.compile(r"<[^>]+>")
        self._re_whitespace = re.compile(r"\s+")

    def fit(self, X):
        return self

    # ---------------------------------------------------------------------
    def _parse_html(self, text: str) -> str:
        """
        Parse cleaned HTML and extract text.
        Removes <a> tags entirely.
        """
        try:
            soup = BeautifulSoup(text, "lxml")

            # Remove links entirely
            for a in soup.find_all("a"):
                a.decompose()

            return soup.get_text(" ")

        except Exception as e:
            self.logger.warning(f"BeautifulSoup parsing failed: {e}")
            return text

    # ---------------------------------------------------------------------
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(df, pd.DataFrame):
            raise ValueError("RemoveHTMLTags.transform expects a pandas DataFrame")

        df = df.copy()

        target_columns = (
            self.columns
            if self.columns is not None
            else df.select_dtypes(include=["object", "string"]).columns
        )

        self.logger.info(f"Applying RemoveHTMLTags to columns: {list(target_columns)}")

        for col in target_columns:
            if col not in df.columns:
                self.logger.warning(f"Column '{col}' not found, skipping")
                continue

            self.logger.info(f"Cleaning HTML tags in column: {col}")

            # Ensure string dtype
            s = df[col].astype(str).fillna("")

            # -----------------------------------------------------------------
            # 1. Decode HTML entities
            # -----------------------------------------------------------------
            self.logger.info(f"Decoding HTML entities in column: {col}")
            s = s.map(html.unescape)

            # -----------------------------------------------------------------
            # 2. Fix malformed/escaped tags (e.g. <\/p>)
            # -----------------------------------------------------------------
            self.logger.info(f"Fixing malformed tags in column: {col}")
            s = s.str.replace(r"<\\/", "</", regex=True)

            # -----------------------------------------------------------------
            # 3. Remove data-embedjson attributes
            # -----------------------------------------------------------------
            self.logger.info(f"Removing data-embedjson attributes in column: {col}")
            s = s.str.replace(self._re_data_embed, "", regex=True)

            # -----------------------------------------------------------------
            # 4. Remove js-embed div/span blocks
            # -----------------------------------------------------------------
            self.logger.info(f"Removing js-embed blocks in column: {col}")
            s = s.str.replace(self._re_embed_block, "", regex=True)

            # -----------------------------------------------------------------
            # 5. Remove scripts and styles
            # -----------------------------------------------------------------
            self.logger.info(f"Removing <script> and <style> blocks in column: {col}")
            s = s.str.replace(self._re_script, "", regex=True)
            s = s.str.replace(self._re_style, "", regex=True)

            # -----------------------------------------------------------------
            # 6. Remove embedded PDF content (common in scraped data)
            # -----------------------------------------------------------------
            self.logger.info(f"Removing embedded PDF content in column: {col}")
            s = s.str.replace(r'%PDF-.*?endobj', '', regex=True | re.DOTALL)
            s = s.str.replace(r'stream[\s\S]*?endstream', '', regex=True)
            s = s.str.replace(r'\d+\s+0\s+obj', '', regex=True)
            s = s.str.replace(r'/[a-zA-Z]+\s*<<.*?>>', '', regex=True | re.DOTALL)  # remove object dictionaries
            s = s.str.replace(r'\/[a-zA-Z]+\s*\/?[a-zA-Z0-9]*', '', regex=True)  # remove PDF keys like /Metadata /Contents

            # -----------------------------------------------------------------
            # 7. Parse HTML (extract text, remove <a> links)
            # -----------------------------------------------------------------
            self.logger.info(f"Parsing HTML and extracting text in column: {col}")
            s = pd.Series([self._parse_html(text) for text in s], index=s.index)

            # -----------------------------------------------------------------
            # 8. Final cleanup
            # -----------------------------------------------------------------
            self.logger.info(f"Performing final cleanup in column: {col}")
            s = s.str.replace(r'[^\x20-\x7E]+', ' ', regex=True)  # remove non-printable characters
            s = s.str.replace(self._re_escaped_tags, " ", regex=True)   # catch <\/p> leftover
            s = s.str.replace(self._re_leftover_tags, " ", regex=True)
            s = s.str.replace(self._re_whitespace, " ", regex=True)


            if self.strip:
                s = s.str.strip()

            df[col] = s

        self.logger.info("Completed RemoveHTMLTags.transform")
        return df

    # ---------------------------------------------------------------------
    def get_params(self) -> dict:
        return {
            "columns": self.columns,
            "strip": self.strip,
        }