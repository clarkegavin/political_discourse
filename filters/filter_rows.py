# filters/filter_rows.py
from typing import Any, Optional
from logs.logger import get_logger
import pandas as pd
from filters.base import Filter


class FilterRows(Filter):
    """
    Flexible row filter. Configurable via params:
      - field: column name to evaluate (required)
      - values: single value or list of values to compare against (optional for range ops)
      - operator: one of [in, not_in, equals, not_equals, gt, gte, lt, lte, contains, regex]
      - case_sensitive: bool for string ops (default True)
      - include: bool - if True keep only rows that match criteria; if False exclude matching rows (default True)

    Examples in YAML:
      - name: filter_rows
        params:
          field: "Release_Date"
          values: [null]
          operator: "in"
          case_sensitive: false
          include: false

    Notes:
      - For comparison operators (gt/gte/lt/lte) this class will attempt numeric comparison first,
        then datetime comparison if numeric fails.
      - `values` can be a list or a single value. For range comparisons provide a single value;
        to compare against multiple values use `in`/`not_in`/`equals`.
    """

    def __init__(
        self,
        field: str,
        values: Optional[Any] = None,
        operator: str = "in",
        case_sensitive: bool = True,
        include: bool = True,
    ):
        self.field = field
        # normalize values to list when appropriate
        if isinstance(values, (list, tuple, set)):
            self.values = list(values)
        elif values is None:
            self.values = None
        else:
            self.values = [values]

        self.operator = operator
        self.case_sensitive = case_sensitive
        self.include = include
        self.logger = get_logger("FilterRows")
        self.is_fitted = False

        self.logger.info(
            f"Initialized FilterRows(field={self.field}, operator={self.operator}, values={self.values}, case_sensitive={self.case_sensitive}, include={self.include})"
        )

    def fit(self, data: pd.DataFrame):
        # Nothing to fit, but ensure field exists
        if self.field not in data.columns:
            self.logger.warning(f"Field '{self.field}' not found in DataFrame columns")
        self.is_fitted = True
        return self

    def _to_series(self, data: pd.DataFrame) -> pd.Series:
        return data[self.field]

    def _attempt_numeric(self, series: pd.Series, val: Any):
        try:
            s_num = pd.to_numeric(series.dropna(), errors="coerce")
            v_num = float(pd.to_numeric(val))
            return True, s_num, v_num
        except Exception:
            return False, None, None

    def _attempt_datetime(self, series: pd.Series, val: Any):
        try:
            s_dt = pd.to_datetime(series, errors="coerce")
            v_dt = pd.to_datetime(val)
            return True, s_dt, v_dt
        except Exception:
            return False, None, None

    def _normalize_mask(self, mask, index):
        """Ensure mask is a pandas Series indexed like the original series and with NaNs filled as False."""
        if isinstance(mask, pd.Series):
            try:
                return mask.reindex(index).fillna(False)
            except Exception:
                return pd.Series(mask.values, index=index).fillna(False)
        # if mask is a scalar or array-like
        try:
            return pd.Series(mask, index=index).fillna(False)
        except Exception:
            return pd.Series([bool(mask)] * len(index), index=index)

    def _build_mask(self, series: pd.Series) -> pd.Series:
        op = self.operator.lower()
        vals = self.values
        # handle missing column
        if series is None:
            return pd.Series([False] * 0)

        # For string ops we may want to lower-case for case-insensitive operations
        if not self.case_sensitive:
            try:
                series_str = series.astype(str).str.lower()
            except Exception:
                series_str = series
        else:
            series_str = series

        # if op in ("in", "not_in"):
        #     if vals is None:
        #         # nothing to compare -> no matches
        #         mask = pd.Series([False] * len(series), index=series.index)
        #     else:
        #         # handle None (null) comparison: python None used in YAML for null
        #         compare_vals = [v for v in vals]
        #         # if case-insensitive and values are strings, lower them
        #         if not self.case_sensitive:
        #             compare_vals = [str(v).lower() if v is not None else v for v in compare_vals]
        #             mask = series_str.isin(compare_vals)
        #         else:
        #             mask = series.isin(compare_vals)
        #     if op == "not_in":
        #         mask = ~mask
        #     return self._normalize_mask(mask, series.index)
        if op in ("in", "not_in"):
            if vals is None:
                mask = pd.Series([False] * len(series), index=series.index)
            else:
                # handle None/null comparison
                mask_nan = series.isna() if any(v is None for v in vals) else pd.Series(False, index=series.index)

                # prepare comparison values for non-null entries
                compare_vals = [v for v in vals if v is not None]
                if not self.case_sensitive:
                    series_comp = series.fillna("").astype(str).str.lower()
                    compare_vals = [str(v).lower() for v in compare_vals]
                else:
                    series_comp = series

                mask_values = series_comp.isin(compare_vals)
                mask = mask_values | mask_nan

            if op == "not_in":
                mask = ~mask
            return self._normalize_mask(mask, series.index)

        if op in ("equals", "not_equals"):
            if vals is None or len(vals) == 0:
                mask = pd.Series([False] * len(series), index=series.index)
            else:
                # equals if any of the provided values match
                if not self.case_sensitive:
                    compare_vals = [str(v).lower() if v is not None else v for v in vals]
                    mask = series_str.isin(compare_vals)
                else:
                    mask = series.isin(vals)
            if op == "not_equals":
                mask = ~mask
            return self._normalize_mask(mask, series.index)

        # comparison ops: gt, gte, lt, lte
        if op in ("gt", "gte", "lt", "lte"):
            if vals is None or len(vals) == 0:
                return pd.Series([False] * len(series), index=series.index)
            v = vals[0]
            # try numeric first
            ok_num, s_num, v_num = self._attempt_numeric(series, v)
            if ok_num:
                # reindex s_num to series index (s_num was dropna)
                s_full = pd.to_numeric(series, errors="coerce")
                if op == "gt":
                    mask = s_full > v_num
                elif op == "gte":
                    mask = s_full >= v_num
                elif op == "lt":
                    mask = s_full < v_num
                else:
                    mask = s_full <= v_num
                return self._normalize_mask(mask, series.index)

            # try datetime
            ok_dt, s_dt, v_dt = self._attempt_datetime(series, v)
            if ok_dt:
                s_full = pd.to_datetime(series, errors="coerce")
                if op == "gt":
                    mask = s_full > v_dt
                elif op == "gte":
                    mask = s_full >= v_dt
                elif op == "lt":
                    mask = s_full < v_dt
                else:
                    mask = s_full <= v_dt
                return self._normalize_mask(mask, series.index)

            # fallback to lexicographic string compare
            try:
                s_str = series.astype(str)
                v_str = str(v)
                if op == "gt":
                    mask = s_str > v_str
                elif op == "gte":
                    mask = s_str >= v_str
                elif op == "lt":
                    mask = s_str < v_str
                else:
                    mask = s_str <= v_str
                return self._normalize_mask(mask, series.index)
            except Exception:
                return pd.Series([False] * len(series), index=series.index)

        if op == "contains":
            if vals is None or len(vals) == 0:
                return pd.Series([False] * len(series), index=series.index)
            v = vals[0]
            try:
                if not self.case_sensitive:
                    mask = series.astype(str).str.contains(str(v), case=False, na=False)
                else:
                    mask = series.astype(str).str.contains(str(v), case=True, na=False)
            except Exception:
                mask = pd.Series([False] * len(series), index=series.index)
            return self._normalize_mask(mask, series.index)

        if op == "regex":
            if vals is None or len(vals) == 0:
                return pd.Series([False] * len(series), index=series.index)
            v = vals[0]
            try:
                mask = series.astype(str).str.contains(v, regex=True, na=False)
            except Exception:
                mask = pd.Series([False] * len(series), index=series.index)
            return self._normalize_mask(mask, series.index)

        # unknown operator -> no matches
        self.logger.warning(f"Unknown operator '{self.operator}'. No rows will be matched.")
        return pd.Series([False] * len(series), index=series.index)

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        if self.field not in data.columns:
            self.logger.warning(f"Field '{self.field}' not found in DataFrame. Returning original data.")
            return data

        series = self._to_series(data)
        mask = self._build_mask(series)

        if self.include:
            result = data[mask]
            self.logger.info(f"Included {mask.sum()} rows matching {self.field} {self.operator} {self.values}")
        else:
            result = data[~mask]
            self.logger.info(f"Excluded {mask.sum()} rows matching {self.field} {self.operator} {self.values}")

        return result.reset_index(drop=True)
