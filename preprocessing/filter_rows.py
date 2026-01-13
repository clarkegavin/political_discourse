# preprocessing/filter_rows.py
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


class FilterRows(Preprocessor):
    """Filter (remove) records from an iterable or pandas.DataFrame based on a field value.

    Designed to work with iterables of dict-like objects or pandas.Series/DataFrame.

    Parameters
    ----------
    field: str
        Name of the field to check on each record (required).
    values: Optional[Iterable[Any]]
        Iterable of values to match against (used with 'equals' or 'in').
    operator: str
        One of: 'equals' (default), 'contains', 'in', 'regex'.
    case_sensitive: bool
        Whether string comparisons should be case-sensitive (default: False)
    negate: bool
        If True, keeps matching rows and removes non-matching; default False removes matches.
    """

    def __init__(
        self,
        field: str,
        values: Optional[Iterable[Any]] = None,
        operator: str = "equals",
        case_sensitive: bool = False,
        negate: bool = False,
    ):
        self.logger = get_logger(self.__class__.__name__)
        self.field = field
        self.values = list(values) if values is not None else []
        self.operator = (operator or "equals").lower()
        self.case_sensitive = bool(case_sensitive)
        self.negate = bool(negate)

        self.logger.info(
            f"Initializing FilterRows field={self.field} operator={self.operator} case_sensitive={self.case_sensitive} negate={self.negate}"
        )

        # compile regexes if requested
        self._regexes = None
        if self.operator == "regex" and self.values:
            try:
                self._regexes = [re.compile(v) for v in self.values]
            except Exception as e:
                self.logger.warning(f"Failed to compile regex values: {e}; regex filtering will be ignored")
                self._regexes = None

    def _get_field(self, item: Any) -> Any:
        # support dict-like and attribute access
        try:
            if isinstance(item, dict):
                return item.get(self.field)
            # pandas.Series has get() and supports indexing with []
            if hasattr(item, "get"):
                try:
                    return item.get(self.field)
                except Exception:
                    pass
            # fallback to attribute
            if hasattr(item, self.field):
                return getattr(item, self.field)
        except Exception:
            pass
        return None

    def _matches(self, candidate: Any) -> bool:
        # perform operator-based matching for a single candidate value
        if not self.case_sensitive and isinstance(candidate, str):
            cand = candidate.lower()
        else:
            cand = candidate

        if self.operator == "equals":
            for v in self.values:
                check = v if self.case_sensitive or not isinstance(v, str) else (v.lower() if isinstance(v, str) else v)
                if cand == check:
                    return True
            return False

        if self.operator == "contains":
            try:
                cs = str(cand) if cand is not None else ""
                for v in self.values:
                    vs = str(v)
                    if not self.case_sensitive:
                        cs = cs.lower()
                        vs = vs.lower()
                    if vs in cs:
                        return True
            except Exception:
                return False
            return False

        if self.operator == "in":
            return any(cand == (v if self.case_sensitive or not isinstance(v, str) else (v.lower() if isinstance(v, str) else v)) for v in self.values)

        if self.operator == "regex" and self._regexes:
            try:
                s = str(cand) if cand is not None else ""
                for rx in self._regexes:
                    if rx.search(s):
                        return True
            except Exception:
                return False
            return False

        # unknown operator -> no match
        return False

    def fit(self, X: Iterable[Any]):
        # stateless
        return self

    def transform(self, X: Iterable[Any]) -> List[Any]:
        """Return filtered records.

        If a pandas.DataFrame is provided (and pandas is available), return a
        filtered DataFrame. Otherwise, return a list of records (same type as
        input items).
        """
        self.logger.info("Applying FilterRows transformation")


        # pandas DataFrame optimisation
        if pd is not None and isinstance(X, pd.DataFrame):
            self.logger.info("Detected pandas.DataFrame input; applying vectorised filtering")
            col = X.get(self.field)
            if col is None:
                self.logger.warning(f"Field '{self.field}' not present in DataFrame; returning original DataFrame")
                return X

            # prepare series for comparisons
            ser = col
            if not self.case_sensitive and ser.dtype == object:
                ser = ser.astype(str).str.lower()

            mask = None

            if self.operator == "equals" or self.operator == "in":
                # values = [v if self.case_sensitive or not isinstance(v, str) else v.lower() for v in self.values]
                # mask = ser.isin(values)
                # Normalise dataframe values
                self.logger.info(f"Operator was {self.operator}; normalising values for comparison")
                # normalize strings if needed
                if not self.case_sensitive:
                    ser_norm = ser.astype(str).str.lower().str.strip()
                    values_norm = [str(v).lower().strip() for v in self.values if v is not None]
                else:
                    ser_norm = ser.astype(str).str.strip()
                    values_norm = [str(v).strip() for v in self.values if v is not None]

                # mask for normal values
                mask_values = ser_norm.isin(values_norm)
                # mask for nulls
                mask_nan = pd.isna(ser) if any(v is None for v in self.values) else pd.Series(False, index=ser.index)

                # final mask
                mask = mask_values | mask_nan

            elif self.operator == "contains":
                # build OR mask for each value
                mask = pd.Series(False, index=X.index)
                for v in self.values:
                    pattern = str(v)
                    if not self.case_sensitive:
                        pattern = pattern.lower()
                        mask = mask | ser.astype(str).str.contains(pattern, case=True, na=False)
                    else:
                        mask = mask | ser.astype(str).str.contains(pattern, case=True, na=False)
            elif self.operator == "regex" and self._regexes:
                mask = pd.Series(False, index=X.index)
                for rx in self._regexes:
                    mask = mask | ser.astype(str).str.contains(rx, na=False)
            else:
                self.logger.warning(f"Unsupported operator '{self.operator}' for DataFrame filtering; returning original DataFrame")
                return X

            # apply negate semantics: if negate False -> remove matches (keep ~mask)
            if self.negate:
                out_df = X[mask]
            else:
                out_df = X[~mask]

            #log the number of rows removed
            removed = len(X) - len(out_df)
            self.logger.info(
                f"FilterRows on field '{self.field}' using operator '{self.operator}': removed {removed} rows, remaining {len(out_df)}"
            )
            #self.logger.info(f"Filtered DataFrame from {len(X)} to {len(out_df)} rows")
            return out_df

        # fallback: iterate over items
        out: List[Any] = []
        count_in = 0
        total = 0
        for i, item in enumerate(X):
            total += 1
            val = self._get_field(item)
            if val is None and not isinstance(item, (dict,)) and not hasattr(item, "get") and not hasattr(item, self.field):
                out.append(item)
                continue

            matches = self._matches(val)
            if matches:
                count_in += 1

            if self.negate:
                if matches:
                    out.append(item)
            else:
                if not matches:
                    out.append(item)

        try:
            self.logger.info(f"Filtered from {total} to {len(out)} records (matched {count_in})")
        except Exception:
            pass
        return out

    def get_params(self) -> dict:
        return {"field": self.field, "operator": self.operator, "case_sensitive": self.case_sensitive, "negate": self.negate}
