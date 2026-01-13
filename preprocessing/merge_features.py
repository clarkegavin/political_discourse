from .base import Preprocessor
from logs.logger import get_logger

class MergeFeatures(Preprocessor):
    """Preprocessor that merges multiple feature columns into a single text column.

    Behaviour:
    - Accepts a list of column names to merge.
    - Joins the values of these columns into a single string per row, separated by spaces.
    - Returns a list of merged strings.
    """

    def __init__(self, columns: list, merge_to: str = "merged_feature"):
        self.columns = columns
        self.merge_to = merge_to
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info(f"Initialized MergeFeatures preprocessor with columns: {self.columns}")


    def fit(self, X):
        # stateless
        return self

    def transform(self, X):
        self.logger.info("Starting MergeFeatures transformation")
        df = X.copy()
        # Merge columns into target column
        df[self.merge_to] = (
            df[self.columns]
            .fillna("")
            .astype(str)
            .agg(" ".join, axis=1)
            .str.strip()
        )

        self.logger.info("Completed MergeFeatures transformation")
        return df

    def get_params(self) -> dict:
        return {'columns': self.columns}