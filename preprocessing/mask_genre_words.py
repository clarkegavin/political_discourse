# preprocessing/mask_genre_words.py
from typing import Iterable, List, Any, Optional
from .base import Preprocessor
from logs.logger import get_logger
import re

# optional pandas support
try:
    import importlib
    pd = importlib.import_module('pandas')
except Exception:
    pd = None


class MaskGenreWords(Preprocessor):
    """Mask words that appear in a genre field from a text/description field.

    Behaviour:
    - Operates primarily on pandas.DataFrame inputs (recommended).
    - If given a DataFrame, it will look up `genre_field` and mask any words
      from that field inside the `description_field` column, replacing them
      with `mask_token`.
    - Matching is done on word boundaries (default) and is case-insensitive
      by default.

    Parameters
    ----------
    genre_field: str
        Name of the column containing the genre to mask (default: 'Genre').
    description_field: str
        Name of the text column to apply masking to (default: 'Description').
    mask_token: str
        Token to replace matches with (default: '<MASKED>').
    case_sensitive: bool
        Whether matching should be case sensitive (default: False).
    split_pattern: Optional[str]
        Regex pattern used to split multi-value genre strings into tokens. By
        default splits on common delimiters and whitespace.
    """

    def __init__(
        self,
        genre_field: str = "Genre",
        description_field: str = "Description",
        mask_token: str = "<MASKED>",
        case_sensitive: bool = False,
        genre_words: Optional[List[str]] = None,
        split_pattern: Optional[str] = r"[,/;&|\-]\s*",
    ):
        self.logger = get_logger(self.__class__.__name__)
        self.description_field = description_field
        self.mask_token = mask_token
        self.case_sensitive = bool(case_sensitive)
        self.split_pattern = split_pattern
        self.genre_words = genre_words or []

        self.logger.info(
            f"Initialized MaskGenreWords(description_field={self.description_field}, mask_token={self.mask_token}, case_sensitive={self.case_sensitive})"
        )

    def _tokens_from_genre(self, genre_value: Any) -> List[str]:
        # Use hardcoded genre words if provided
        if self.genre_words:
            return self.genre_words
        # fallback to splitting genre_value
        if genre_value is None:
            return []
        s = str(genre_value)
        parts = re.split(self.split_pattern, s) if self.split_pattern else [s]
        tokens: List[str] = []
        for p in parts:
            for tok in p.strip().split():
                if tok:
                    tokens.append(tok)
        # deduplicate
        seen = set()
        out = []
        for t in tokens:
            key = t if self.case_sensitive else t.lower()
            if key not in seen:
                seen.add(key)
                out.append(t)
        return out

    def _mask_description(self, description: str, tokens: List[str]) -> str:
        if not description or not tokens:
            return description
        out = description
        flags = 0
        if not self.case_sensitive:
            flags = re.IGNORECASE
        # sort tokens by length desc to avoid partial masking of longer tokens
        tokens_sorted = sorted(tokens, key=lambda x: -len(x))
        for tok in tokens_sorted:
            if not tok:
                continue
            try:
                # use word boundary to match whole words; escape token
                #pattern = r"\b" + re.escape(tok) + r"\b"
                pattern = r"\b" + re.escape(tok) + r"\w*\b"
                out = re.sub(pattern, self.mask_token, out, flags=flags)
            except re.error:
                # fallback: simple replace
                if self.case_sensitive:
                    out = out.replace(tok, self.mask_token)
                else:
                    # case-insensitive replace via regex
                    out = re.sub(re.escape(tok), self.mask_token, out, flags=re.IGNORECASE)

        return out

    def fit(self, X: Iterable[Any]):
        # stateless
        return self

    def transform(self, X: Iterable[Any]) -> List[Any]:
        """Transform data.

        If X is a pandas.DataFrame (and pandas is available) the DataFrame with
        masked description column is returned. Otherwise, if given an iterable
        of non-DataFrame records this preprocessor will attempt no-op and log
        a warning (because masking requires access to the genre field).
        """
        self.logger.info("Applying MaskGenreWords transform")
        self.logger.info(f"Input type: {type(X)}")
        if pd is not None and isinstance(X, pd.DataFrame):
            self.logger.info("Detected pandas DataFrame input for MaskGenreWords")
            df = X.copy()
            if self.genre_field not in df.columns:
                self.logger.warning(f"Genre field '{self.genre_field}' not in DataFrame; returning original DataFrame")
                return df
            if self.description_field not in df.columns:
                self.logger.warning(f"Description field '{self.description_field}' not in DataFrame; returning original DataFrame")
                return df

            def _mask_row(row):
                self.logger.info(f"Masking row with genre: {row[self.genre_field]}")
                try:
                    tokens = self._tokens_from_genre(row[self.genre_field])
                    return self._mask_description(row[self.description_field], tokens)
                except Exception as e:
                    self.logger.warning(f"Masking failed for row: {e}")
                    return row[self.description_field]

            df[self.description_field] = df.apply(_mask_row, axis=1)

            self.logger.info(f"Example masked descriptions: {df[self.description_field].head().tolist()}")
            return df

        # fallback: iterable of dict-like objects -> try to process in place
        # but only if each item contains both fields
        out = []
        any_processed = False
        for item in X:
            try:
                # dict-like
                if isinstance(item, dict) and self.genre_field in item and self.description_field in item:
                    tokens = self._tokens_from_genre(item.get(self.genre_field))
                    item_copy = dict(item)
                    item_copy[self.description_field] = self._mask_description(item_copy.get(self.description_field, ""), tokens)
                    out.append(item_copy)
                    any_processed = True
                    continue
                # pandas.Series like
                if hasattr(item, 'get') and self.genre_field in item and self.description_field in item:
                    tokens = self._tokens_from_genre(item.get(self.genre_field))
                    try:
                        new_item = dict(item)
                        new_item[self.description_field] = self._mask_description(item.get(self.description_field, ""), tokens)
                        out.append(new_item)
                        any_processed = True
                        continue
                    except Exception:
                        pass
                # otherwise, we cannot mask without genre info -> append original
                out.append(item)
            except Exception:
                out.append(item)
        if any_processed:
            return out

        # lastly, if we have a simple iterable of strings and hardcoded genre words
        if self.genre_words and all(isinstance(x, str) for x in X):
            tokens = self._tokens_from_genre(None)  # returns self.genre_words
            return [self._mask_description(text, tokens) for text in X]

        self.logger.warning("MaskGenreWords received input it cannot process (expected DataFrame or iterable of dict-like records); returning original input as list")
        return list(X)

    def get_params(self) -> dict:
        return {
            "genre_field": self.genre_field,
            "description_field": self.description_field,
            "mask_token": self.mask_token,
            "case_sensitive": self.case_sensitive,
        }
