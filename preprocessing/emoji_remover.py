# preprocessing/emoji_remover.py
from typing import Iterable, List, Optional, Pattern
from .base import Preprocessor
from logs.logger import get_logger
import re

# dynamic import for the optional `emoji` package
try:
    import importlib
    _emoji_mod = importlib.import_module("emoji")
except Exception:
    _emoji_mod = None


class EmojiRemover(Preprocessor):
    """Removes emoji characters from text.

    Two configurable approaches are supported:
    - regexes: list of regex strings to remove (applied first)
    - use_emoji_lib: if True and the `emoji` package is available, use its
      utilities to strip emoji (applied after regex removals)

    Parameters
    ----------
    regexes: Optional[List[str]]
        List of regex strings. If provided they will be compiled and applied
        to remove matches.
    use_emoji_lib: bool
        Whether to attempt to use the external `emoji` package when available.
    """

    def __init__(self, regexes: Optional[List[str]] = None, use_emoji_lib: bool = True):
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info(f"Initializing EmojiRemover use_emoji_lib={use_emoji_lib} regexes={bool(regexes)}")
        self.use_emoji_lib = bool(use_emoji_lib)
        self._emoji_mod = _emoji_mod if self.use_emoji_lib else None

        # compile a combined regex if regexes provided
        self._compiled: Optional[Pattern] = None
        if regexes:
            try:
                combined = "|".join(f"(?:{r})" for r in regexes)
                self._compiled = re.compile(combined, flags=re.UNICODE)
                self.logger.info(f"Compiled {len(regexes)} emoji regex patterns")
            except Exception as e:
                self.logger.warning(f"Failed to compile provided regexes: {e}; ignoring regexes")
                self._compiled = None

    def fit(self, X: Iterable[str]):
        # stateless
        return self

    def transform(self, X: Iterable[str]) -> List[str]:
        self.logger.info("Applying EmojiRemover transformation")
        out: List[str] = []

        for i, doc in enumerate(X):
            s = "" if doc is None else str(doc)

            # apply user-provided regex removals first
            if self._compiled is not None:
                try:
                    s = self._compiled.sub("", s)
                except Exception as e:
                    self.logger.warning(f"Regex-based emoji removal failed on index {i}: {e}")

            # then try emoji library if available
            if self._emoji_mod is not None:
                try:
                    # newer emoji package provides replace_emoji
                    if hasattr(self._emoji_mod, "replace_emoji"):
                        s = self._emoji_mod.replace_emoji(s, replace="")
                    # fallback to get_emoji_regexp if available
                    elif hasattr(self._emoji_mod, "get_emoji_regexp"):
                        regexp = self._emoji_mod.get_emoji_regexp()
                        s = regexp.sub("", s)
                    else:
                        # last resort: attempt to remove characters in emoji.EMOJI_DATA (if present)
                        emoji_data = getattr(self._emoji_mod, "EMOJI_DATA", None)
                        if emoji_data is not None and isinstance(emoji_data, dict):
                            # build char class from keys (may be large) -- guard with try
                            try:
                                chars = "".join(re.escape(c) for c in emoji_data.keys())
                                fallback_re = re.compile(f"[{chars}]")
                                s = fallback_re.sub("", s)
                            except Exception:
                                # give up silently
                                pass
                except Exception as e:
                    self.logger.warning(f"Emoji library based removal failed on index {i}: {e}")

            out.append(s)

            if i < 3:
                try:
                    self.logger.info(f"Original: {doc.encode('utf-8', errors='ignore')}")
                    self.logger.info(f"Cleaned : {s.encode('utf-8', errors='ignore')}")
                except Exception:
                    pass

        self.logger.info("Completed EmojiRemover transformation")
        return out

    def get_params(self) -> dict:
        return {"use_emoji_lib": self.use_emoji_lib, "has_regex": self._compiled is not None}

