# filters/drop_columns_filter.py
from logs.logger import get_logger
import pandas as pd
from filters.base import Filter

class DropColumnsFilter(Filter):
    """
    Filter that drops specified columns from a DataFrame.
    """
    def __init__(self, columns_to_drop):
        self.columns_to_drop = columns_to_drop
        self.is_fitted = False
        self.logger = get_logger("DropColumnsFilter")
        self.logger.info(f"Initialized DropColumnsFilter with columns to drop: {self.columns_to_drop}")

    def fit(self, data: pd.DataFrame):
        # No fitting necessary for dropping columns
        self.is_fitted = True
        self.logger.info("Fit called - no action needed for DropColumnsFilter.")
        return self

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        self.logger.info(f"Dropping columns: {self.columns_to_drop}")
        data =  data.drop(columns=self.columns_to_drop, errors='ignore')
        self.logger.info(f"Dropped columns. Remaining columns: {data.columns.tolist()}")
        return data