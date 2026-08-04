#preprocessing/duplicate_columns.py
from .base import Preprocessor
from logs.logger import get_logger

class DuplicateColumn(Preprocessor):
    """Preprocessor to duplicate a specified column in a dataset with a new name"""

    def __init__(self, source_column: str, target_column: str):
        self.logger = get_logger(self.__class__.__name__)
        self.source_column = source_column
        self.target_column = target_column
        self.logger.info(f"Initialized DuplicateColumnsRemover to duplicate column '{self.source_column}' as '{self.target_column}'")

    def fit(self, X):
        # Stateless preprocessor; nothing to fit
        return self

    def transform(self, X):
        """Duplicate the specified column in the dataset."""
        if self.source_column not in X.columns:
            raise ValueError(f"Column '{self.source_column}' not found in the dataset.")

        # check that the new column name does not already exist
        if self.target_column in X.columns:
            raise ValueError(f"New column name '{self.target_column}' already exists in the dataset.")

        X[self.target_column] = X[self.source_column]
        self.logger.info(f"Duplicated column '{self.source_column}' as '{self.target_column}'")
        return X
