# preprocessing/remove_duplicates.py
from typing import Iterable, List, Any, Optional
from .base import Preprocessor
from logs.logger import get_logger

# optional pandas support
try:
    import importlib
    pd = importlib.import_module('pandas')
except Exception:
    pd = None


class RemoveDuplicates(Preprocessor):
    """Preprocessor that removes duplicate records based on a specified field.

    Behaviour:
    - If given a pandas.DataFrame, uses DataFrame.drop_duplicates on the
      provided `field` (subset) and returns the filtered DataFrame.
    - For iterable/dict-like inputs, iterates and keeps the first occurrence
      of each unique key value (configurable case sensitivity).

    Parameters
    ----------
    field: str
        The column/field to check duplicates against (required).
    keep: str
        Which duplicate to keep: 'first' or 'last' (default 'first').
    case_sensitive: bool
        Whether string comparisons should be case-sensitive (default False).
    dropna: bool
        If True, treat NA/None as a value and deduplicate them; if False,
        do not consider None/NaN values when deduplicating (default True).
    """

    def __init__(
        self,
        field: str,
        keep: str = "first",
        case_sensitive: bool = False,
        dropna: bool = True,
    ):
        if not field:
            raise ValueError("'field' parameter is required for RemoveDuplicates")

        self.logger = get_logger(self.__class__.__name__)
        self.field = field
        self.keep = keep if keep in ("first", "last") else "first"
        self.case_sensitive = bool(case_sensitive)
        self.dropna = bool(dropna)

        self.logger.info(
            f"Initialized RemoveDuplicates(field={self.field}, keep={self.keep}, case_sensitive={self.case_sensitive}, dropna={self.dropna})"
        )

    def fit(self, X: Iterable[Any]):
        # stateless preprocessor
        return self

    def transform(self, X: Iterable[Any]) -> Any:
        self.logger.info(f"Applying RemoveDuplicates on field '{self.field}'")

        # pandas.DataFrame path
        if pd is not None and isinstance(X, pd.DataFrame):
            df = X.copy()
            if self.field not in df.columns:
                self.logger.warning(f"Field '{self.field}' not present in DataFrame; returning original DataFrame")
                return df

            # Optionally remove NA values from consideration by temporarily
            # marking them so they are not considered duplicates when dropna=False
            if not self.dropna:
                # we'll treat NA as unique by mapping them to a unique placeholder
                placeholder = object()
                ser = df[self.field].where(df[self.field].notna(), other=placeholder)
                df = df.assign(_dedupe_key=ser)
                subset = ["_dedupe_key"]
                res = df.drop_duplicates(subset=subset, keep=self.keep).drop(columns=["_dedupe_key"]).reset_index(drop=True)
                self.logger.info(f"Removed duplicates; {len(X) - len(res)} rows dropped")
                return res

            # Normal path: use pandas.drop_duplicates with subset
            res = df.drop_duplicates(subset=[self.field], keep=self.keep).reset_index(drop=True)
            self.logger.info(f"Removed duplicates; {len(X) - len(res)} rows dropped")
            return res

        # fallback: iterable of dict-like objects or other sequences
        seen = set()
        out: List[Any] = []

        def _key(v: Any):
            if v is None:
                return None
            if isinstance(v, str) and not self.case_sensitive:
                return v.lower()
            return v

        for item in X:
            try:
                # dict-like
                if isinstance(item, dict) and self.field in item:
                    val = item.get(self.field)
                elif hasattr(item, 'get'):
                    try:
                        val = item.get(self.field)
                    except Exception:
                        val = getattr(item, self.field, None)
                else:
                    val = getattr(item, self.field, None)

                # handle dropna: if dropna False and val is None, treat as unique
                if val is None and not self.dropna:
                    unique_key = object()
                    seen_key = unique_key
                else:
                    seen_key = _key(val)

                if seen_key in seen:
                    # if keeping last, we need to replace previous entry — naive
                    # approach: if keep == 'last', remove previous and append current
                    if self.keep == 'last':
                        # find and remove previous occurrence in out
                        for i, existing in enumerate(out):
                            existing_val = None
                            if isinstance(existing, dict):
                                existing_val = existing.get(self.field)
                            else:
                                existing_val = getattr(existing, self.field, None)
                            if _key(existing_val) == seen_key:
                                out.pop(i)
                                break
                        out.append(item)
                    else:
                        # keep == 'first' -> skip
                        continue
                else:
                    seen.add(seen_key)
                    out.append(item)
            except Exception:
                # if anything goes wrong, keep the item (fail-safe)
                out.append(item)

        self.logger.info(f"Removed duplicates from iterable; output size {len(out)}")
        return out

    def get_params(self) -> dict:
        return {"field": self.field, "keep": self.keep, "case_sensitive": self.case_sensitive, "dropna": self.dropna}

