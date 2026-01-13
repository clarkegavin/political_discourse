# pipelines/data_splitter_pipeline.py
from logs.logger import get_logger
from pipelines.base import Pipeline
import pandas as pd
from sklearn.model_selection import train_test_split
from typing import Optional

class DataSplitterPipeline(Pipeline):
    """
    ETL pipeline for splitting a dataset into training, validation, and test sets.
    Inherits from the base Pipeline class.
    """

    def __init__(self, target_column, test_size=0.2, random_state=42):
        self.target_column = target_column
        self.test_size = test_size
        self.random_state = random_state
        self.logger = get_logger(self.__class__.__name__)

    def execute(self, data: Optional[pd.DataFrame] = None):
        if data is None:
            raise ValueError("Data must be provided to DataSplitterPipeline.")

        X = data.drop(columns=[self.target_column])
        y = data[self.target_column]
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=self.test_size, random_state=self.random_state
        )

        self.logger.info(f"X_train shape: {X_train.shape}, X_test shape: {X_test.shape}")
        self.logger.info(f"y_train shape: {y_train.shape}, y_test shape: {y_test.shape}")

        return {"X_train": X_train, "X_test": X_test, "y_train": y_train, "y_test": y_test}
