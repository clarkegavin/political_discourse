# preprocessing/remove_urls.py
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


class RemoveURLs(Preprocessor):
    """Preprocessor that removes URLs from a specified text field.

    Parameters
    ----------
    field: str
        The name of the field/column to remove URLs from (required).
    pattern: str
        Regex pattern used to identify URLs. Default removes http(s) and www links.
    replace_with: str
        Replacement string for matched URLs (default: empty string).
    """

    def __init__(
        self,
        field: str,
        pattern: str = r"https?://\S+|www\.\S+",
        replace_with: str = "",
    ):
        if not field:
            raise ValueError("'field' parameter is required for RemoveURLs")

        self.logger = get_logger(self.__class__.__name__)
        self.field = field
        self.pattern = pattern
        self.replace_with = replace_with
        try:
            self._compiled = re.compile(self.pattern, flags=re.IGNORECASE)
        except Exception:
            self._compiled = None
            self.logger.warning("Failed to compile URL regex; falling back to simple replace")

        self.logger.info(f"Initialized RemoveURLs(field={self.field} pattern={self.pattern})")

    def fit(self, X: Iterable[Any]):
        # stateless
        return self

    def _remove_from_text(self, text: Any) -> str:
        s = "" if text is None else str(text)
        try:
            if self._compiled:
                return self._compiled.sub(self.replace_with, s)
            return re.sub(self.pattern, self.replace_with, s, flags=re.IGNORECASE)
        except Exception:
            return s

    def transform(self, X: Iterable[Any]) -> Any:
        self.logger.info(f"Applying RemoveURLs on field '{self.field}'")

        # DataFrame path
        if pd is not None and isinstance(X, pd.DataFrame):
            df = X.copy()
            if self.field not in df.columns:
                self.logger.warning(f"Field '{self.field}' not present in DataFrame; returning original DataFrame")
                return df

            try:
                # use pandas vectorised replace where possible
                try:
                    df[self.field] = df[self.field].astype(str).replace(self._compiled or self.pattern, self.replace_with, regex=True)
                except Exception:
                    # fallback to apply
                    df[self.field] = df[self.field].apply(self._remove_from_text)
            except Exception as e:
                self.logger.warning(f"Failed to remove URLs via vectorised replace; falling back to per-row apply: {e}")
                df[self.field] = df[self.field].apply(self._remove_from_text)

            return df

        # Iterable/dict-like path
        out: List[Any] = []
        for item in X:
            try:
                if isinstance(item, dict) and self.field in item:
                    copy = dict(item)
                    copy[self.field] = self._remove_from_text(copy.get(self.field, ""))
                    out.append(copy)
                elif hasattr(item, 'get') and self.field in item:
                    # pandas.Series-like
                    try:
                        new_item = dict(item)
                        new_item[self.field] = self._remove_from_text(item.get(self.field, ""))
                        out.append(new_item)
                    except Exception:
                        out.append(item)
                else:
                    # can't find field => leave unchanged
                    out.append(item)
            except Exception:
                out.append(item)
        return out

    def get_params(self) -> dict:
        return {"field": self.field, "pattern": self.pattern, "replace_with": self.replace_with}

