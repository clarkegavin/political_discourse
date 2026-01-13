# preprocessing/temporal_features.py
from .base import Preprocessor
from logs.logger import get_logger
import pandas as pd
from typing import Optional


class TemporalFeatures(Preprocessor):
    """
    Generate temporal features from a date column in a DataFrame.

    Parameters (constructor):
    - date_column: str - name of the date column to parse
    - day: bool (default True) - create a column with day of month
    - month: bool (default True) - create a column with month
    - year: bool (default True) - create a column with year
    - quarter: bool (default False) - create a column with quarter as 'Q1'..'Q4'
    - days_since: bool (default False) - add a column with days between date and reference_date (or today)
    - reference_date: optional str or pandas.Timestamp - if provided used for days_since calculation; if 'now' or None uses pd.Timestamp.now()
    - date_format: optional str - format passed to pd.to_datetime(format=...) if you want to enforce a format
    - prefix: optional str - prefix for new column names; if None or empty, uses the date_column name as prefix

    The transform returns the DataFrame with the new columns added. Missing/invalid dates are kept as NaN.
    """

    def __init__(self, date_column: str,
                 day: bool = True,
                 month: bool = True,
                 year: bool = True,
                 quarter: bool = False,
                 quarter_format: str = 'label',
                 days_since: bool = False,
                 reference_date: Optional[str] = None,
                 date_format: Optional[str] = None,
                 prefix: Optional[str] = None):
        self.date_column = date_column
        self.day = bool(day)
        self.month = bool(month)
        self.year = bool(year)
        self.quarter = bool(quarter)
        # quarter_format: 'label' -> 'Q1'..'Q4'; 'int' -> 1..4 (nullable Int64)
        self.quarter_format = quarter_format if quarter_format in ('label', 'int') else 'label'
        if quarter_format not in ('label', 'int'):
            self.logger = get_logger(self.__class__.__name__)
            self.logger.warning(f"Unknown quarter_format '{quarter_format}' - defaulting to 'label'")
        self.days_since = bool(days_since)
        self.reference_date = reference_date
        self.date_format = date_format
        self.prefix = prefix or date_column
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info(f"Initialized TemporalFeatures for '{self.date_column}' (day={self.day}, month={self.month}, year={self.year}, quarter={self.quarter}, quarter_format={self.quarter_format}, days_since={self.days_since}, reference_date={self.reference_date}, prefix={self.prefix})")

    def fit(self, X):
        # Stateless preprocessor
        return self

    def _parse_dates(self, ser: pd.Series) -> pd.Series:
        # Use date_format if provided to speed parsing, otherwise infer
        try:
            if self.date_format:
                dt = pd.to_datetime(ser, format=self.date_format, errors='coerce')
            else:
                dt = pd.to_datetime(ser, errors='coerce', infer_datetime_format=True)
        except Exception as e:
            self.logger.warning(f"Date parsing with format failed for column {self.date_column}: {e}; falling back to generic parse")
            dt = pd.to_datetime(ser, errors='coerce')
        return dt

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(X, pd.DataFrame):
            self.logger.error("TemporalFeatures.transform expects a pandas DataFrame")
            raise ValueError("TemporalFeatures.transform expects a pandas DataFrame")

        self.logger.info("Starting transform in TemporalFeatures")
        df = X.copy()
        col = self.date_column
        if col not in df.columns:
            self.logger.info(f"Date column '{col}' not found in DataFrame - nothing to do")
            return df

        self.logger.info(f"Generating temporal features from column: {col}")
        ser = df[col]
        dt = self._parse_dates(ser)

        # add components
        if self.day:
            out_col = f"{self.prefix}_day"
            try:
                df.loc[:, out_col] = dt.dt.day
            except Exception as e:
                self.logger.warning(f"Failed to write day for '{col}': {e}")

        if self.month:
            out_col = f"{self.prefix}_month"
            try:
                df.loc[:, out_col] = dt.dt.month
            except Exception as e:
                self.logger.warning(f"Failed to write month for '{col}': {e}")

        if self.year:
            out_col = f"{self.prefix}_year"
            try:
                df.loc[:, out_col] = dt.dt.year
            except Exception as e:
                self.logger.warning(f"Failed to write year for '{col}': {e}")

        if self.quarter:
            out_col = f"{self.prefix}_quarter"
            try:
                # dt.dt.quarter returns 1..4 or NaN for NaT
                q = dt.dt.quarter
                if self.quarter_format == 'label':
                    # Map to 'Q1'..'Q4' safely handling NaN values
                    q_mapped = q.apply(lambda v: f"Q{int(v)}" if pd.notna(v) else pd.NA)
                    df.loc[:, out_col] = q_mapped.astype(object)
                else:
                    # integer quarters, use pandas nullable integer dtype to preserve NA
                    df.loc[:, out_col] = q.astype('Int64')
            except Exception as e:
                self.logger.warning(f"Failed to write quarter for '{col}': {e}")

        if self.days_since:
            out_col = f"{self.prefix}_days_since"
            # reference date
            if self.reference_date in (None, 'now'):
                ref = pd.Timestamp.now()
            else:
                try:
                    ref = pd.to_datetime(self.reference_date)
                except Exception:
                    ref = pd.Timestamp.now()
            try:
                # compute difference in days (float) and cast to Int where possible
                diff = (ref - dt).dt.days
                # where dt is NaT, result will be NaN -> keep as NaN
                df.loc[:, out_col] = diff.astype('float')
            except Exception as e:
                self.logger.warning(f"Failed to compute days_since for '{col}': {e}")

        self.logger.info(f"Temporal features added for '{col}' (prefix={self.prefix})")
        return df

    def fit_transform(self, X):
        return self.fit(X).transform(X)

    def get_params(self):
        return {"date_column": self.date_column, "day": self.day, "month": self.month, "year": self.year, "quarter": self.quarter, "quarter_format": self.quarter_format, "days_since": self.days_since, "reference_date": self.reference_date, "date_format": self.date_format, "prefix": self.prefix}


# alias
Temporal = TemporalFeatures
