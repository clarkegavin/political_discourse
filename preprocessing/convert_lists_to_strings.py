from logs.logger import get_logger
from preprocessing.base import Preprocessor


class ConvertListsToStrings(Preprocessor):
    """
    Preprocessor to convert list columns in a DataFrame to string representations.
    """

    def __init__(self, list_columns = None):
        self.list_columns = list_columns or None
        self.logger = get_logger(self.__class__.__name__)

    def fit(self, data, **kwargs):
        """
        Fit method for compatibility. No fitting required for this preprocessor.
        """
        return self

    def transform(self, data, **kwargs):
        """
        Transform method to convert list columns to strings.

        Args:
            data (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame with specified list columns converted to strings.
        """
        if self.list_columns is None:
            # check for list columns in the DataFrame
            self.list_columns = [col for col in data.columns if data[col].apply(lambda x: isinstance(x, list)).any()]

        self.logger.info("Starting ConvertListsToStrings preprocessor.")
        for col in self.list_columns:
            if col in data.columns:
                self.logger.info(f"Converting column '{col}' from list to string.")
                data[col] = data[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
            else:
                self.logger.warning(f"Column '{col}' not found in DataFrame.")
        return data