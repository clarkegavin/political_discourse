from preprocessing.base import Preprocessor
import pandas as pd
from logs.logger import get_logger
from typing import Optional

class CommentStatistics(Preprocessor):
    """
    Preprocessor to calculate statistics for each individual comment.
    Calculates word count, character count, and optionally sentence count.
    """

    def __init__(
        self,
        text_column: str = "CommentBody",
        word_count_column: str = "CommentWordCount",
        character_count_column: str = "CommentCharacterCount",
        sentence_count_column: Optional[str] = "CommentSentenceCount",
        calculate_sentences: bool = True,
    ):
        self.text_column = text_column
        self.word_count_column = word_count_column
        self.character_count_column = character_count_column
        self.sentence_count_column = sentence_count_column
        self.calculate_sentences = calculate_sentences
        self.logger = get_logger(self.__class__.__name__)

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None):
        """
        Fit method for compatibility. Does nothing and returns self.
        """
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transform method to calculate comment statistics.

        Args:
            X (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame with additional columns for statistics.
        """
        self.logger.info("Starting CommentStatistics transform.")

        # Validate input column
        if self.text_column not in X.columns:
            raise ValueError(f"Input DataFrame must contain the column '{self.text_column}'.")

        # Handle null values in the text column
        text_data = X[self.text_column].fillna("")

        # Calculate word count
        X[self.word_count_column] = text_data.str.split().str.len()

        # Calculate character count
        X[self.character_count_column] = text_data.str.len()

        # Calculate sentence count if enabled
        if self.calculate_sentences and self.sentence_count_column:
            X[self.sentence_count_column] = text_data.str.count(r'[.!?]')

        self.logger.info(
            "Completed CommentStatistics transform. Processed %d records.", len(X)
        )

        return X

    def get_params(self, deep: bool = True):
        """
        Get parameters for this preprocessor.

        Args:
            deep (bool): Whether to return deep copy of parameters.

        Returns:
            dict: Parameters of the preprocessor.
        """
        return {
            "text_column": self.text_column,
            "word_count_column": self.word_count_column,
            "character_count_column": self.character_count_column,
            "sentence_count_column": self.sentence_count_column,
            "calculate_sentences": self.calculate_sentences,
        }
