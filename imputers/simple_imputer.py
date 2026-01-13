# imputers/simple_imputer.py
from .base import Imputer
from logs.logger import get_logger
import pandas as pd
from typing import Dict, Any, Optional, List


class SimpleImputer(Imputer):
    """Simple imputer that fills missing values per-column.

    Behavior:
    - For numeric columns: fill missing values with column mean (computed on fit or computed on the fly)
    - For object/string columns: fill missing values with empty string ''

    Parameters
    - columns: optional list of columns to target; if omitted, operates on all DataFrame columns
    - numeric_strategy: optional, currently 'mean' and 'zero' supported
    - filter_column: optional column name used to select rows to compute statistics and to apply imputation
    - filter_value: optional value (or list) used with filter_column for equality matching
    """

    def __init__(self, columns: Optional[List[str]] = None, numeric_strategy: str = 'mean', text_strategy: str = '', filter_column: Optional[str] = None, filter_value: Optional[Any] = None):
        """Create a SimpleImputer.

        Parameters:
        - columns: list of column names to impute (None => all columns)
        - numeric_strategy: 'mean' or 'zero'
        - replace_with: string to use when imputing text/missing non-numeric values
        - filter_column: optional column name used to select which rows are considered when computing statistics and where imputation is applied
        - filter_value: value or list of values to match in filter_column (equality / isin match)
        """
        self.columns = columns
        self.numeric_strategy = numeric_strategy
        self.replace_with = text_strategy
        self.filter_column = filter_column
        self.filter_value = filter_value
        self.statistics_: Dict[str, Any] = {}
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info(f"Initialized SimpleImputer(columns={self.columns}, numeric_strategy={self.numeric_strategy}, replace_with={self.replace_with}, filter_column={self.filter_column}, filter_value={self.filter_value})")

    def fit(self, X: pd.DataFrame):
        self.logger.info("Imputer - Starting fit")
        if not isinstance(X, pd.DataFrame):
            raise ValueError("SimpleImputer.fit expects a pandas DataFrame")

        cols = self.columns or list(X.columns)
        self.logger.info(f"Imputer - Fitting columns: {cols}")

        # build mask for filtering if requested
        if self.filter_column and self.filter_column in X.columns:
            if isinstance(self.filter_value, (list, tuple, set)):
                mask = X[self.filter_column].isin(list(self.filter_value))
            else:
                #mask = X[self.filter_column] == self.filter_value
                mask = X[self.filter_column].astype(str) == str(self.filter_value)
            self.logger.info(f"Imputer - Using filter on column '{self.filter_column}' for fit with value(s)={self.filter_value}")
        else:
            mask = pd.Series([True] * len(X), index=X.index)
            if self.filter_column:
                self.logger.warning(f"Imputer - filter_column '{self.filter_column}' not found in DataFrame; proceeding without filter")

        self.logger.info(f"Imputer Mask Summary: {mask.sum()} rows selected out of {len(X)} total")

        for c in cols:
            self.logger.info(f"Imputer - Processing column '{c}'")
            if c not in X.columns:
                self.logger.warning(f"Column '{c}' not found in DataFrame during fit; skipping")
                continue

            ser = X.loc[mask, c]
            # log type of column
            self.logger.info(f"Fitting column '{c}' of type {ser.dtype}")
            if pd.api.types.is_numeric_dtype(X[c]):
                if self.numeric_strategy == 'mean':
                    # compute mean on the filtered subset
                    self.logger.info(f"Scaling: Computing mean for numeric column '{c}'")
                    mean_val = ser.dropna().astype(float).mean()
                    self.statistics_[c] = mean_val
                elif self.numeric_strategy == 'zero':
                    self.logger.info(f"Scaling: Using zero for numeric column '{c}'")
                    self.statistics_[c] = 0
                else:
                    self.logger.warning(f"Unknown numeric_strategy '{self.numeric_strategy}' - skipping numeric stat for {c}")
            else:
                # For non-numeric we will use configured replace_with (default '')
                self.statistics_[c] = self.replace_with

        self.logger.info(f"Computed statistics for imputation: {self.statistics_}")
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Imputer - Starting transform")
        if not isinstance(X, pd.DataFrame):
            raise ValueError("SimpleImputer.transform expects a pandas DataFrame")

        df = X.copy()
        cols = self.columns or list(df.columns)

        # build mask for filtering if requested
        if self.filter_column and self.filter_column in df.columns:
            if isinstance(self.filter_value, (list, tuple, set)):
                mask = df[self.filter_column].isin(list(self.filter_value))
            else:
                #mask = df[self.filter_column] == self.filter_value
                mask = df[self.filter_column].astype(str) == str(self.filter_value)
            self.logger.info(f"Imputer - Using filter on column '{self.filter_column}' for transform with value(s)={self.filter_value}")
        else:
            mask = pd.Series([True] * len(df), index=df.index)
            if self.filter_column:
                self.logger.warning(f"Imputer - filter_column '{self.filter_column}' not found in DataFrame; proceeding without filter")

        for c in cols:
            if c not in df.columns:
                self.logger.debug(f"Column {c} missing from DataFrame; skipping")
                continue

            stat = self.statistics_.get(c, None)
            if stat is None:
                # compute on the filtered subset (if any) or fallback to overall
                ser = df.loc[mask, c] if mask.any() else df[c]
                if pd.api.types.is_numeric_dtype(df[c]):
                    if self.numeric_strategy == 'mean':
                        self.logger.info(f"Transform: Computing mean for numeric column '{c}' on the fly")
                    stat = ser.dropna().astype(float).mean()
                else:

                    stat = self.replace_with

            try:
                if mask.all():
                    # no filtering, fill entire column
                    df[c] = df[c].fillna(stat)
                else:
                    # only fill rows matching the mask
                    try:
                        df.loc[mask, c] = df.loc[mask, c].fillna(stat)
                    except Exception:
                        # fallback in case of unexpected types
                        self.logger.info(f"Filling column {c} with {stat} using apply fallback")
                        df.loc[mask, c] = df.loc[mask, c].apply(lambda v: stat if pd.isna(v) else v)
            except Exception as e:
                self.logger.warning(f"Failed to fill column {c} with {stat}: {e}")

        # --- LOGGING NaNs AFTER IMPUTATION ---
        for c in cols:
            n_missing = df[c].isna().sum()
            if n_missing > 0:
                self.logger.warning(f"Column '{c}' still has {n_missing} NaN values AFTER imputation")

        return df

    def fit_transform(self, X: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Imputer - Starting fit_transform")
        return self.fit(X).transform(X)

    def get_params(self):
        return {'columns': self.columns, 'numeric_strategy': self.numeric_strategy, 'replace_with': self.replace_with, 'filter_column': self.filter_column, 'filter_value': self.filter_value}
