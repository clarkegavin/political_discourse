import re
from typing import List, Optional
import pandas as pd
from .base import Preprocessor
from logs.logger import get_logger


class ReplyReferenceExtractor(Preprocessor):
    def __init__(self, columns: Optional[List[str]] = None, output_column: str = "ReplyToIds", extract_multiple: bool = True):
        self.columns = columns
        self.output_column = output_column
        self.extract_multiple = extract_multiple
        self.pattern = re.compile(r'(?:&quot;|")recordID(?:&quot;|"):(\d+)')
        self.logger = get_logger(self.__class__.__name__)

    def fit(self, X):
        return self


    def transform(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        self.logger.info("Running ReplyReferenceExtractor on columns: %s", self.columns or "all text columns")

        if self.columns is None:
            self.columns = data.select_dtypes(include=["object", "string"]).columns.tolist()
        sample_matches = []
        
        def extract_ids(text):
            if not isinstance(text, str):
                return []
            matches = self.pattern.findall(text)

            if matches and len(sample_matches) < 5:
                sample_matches.extend(matches)
            return list(map(int, matches)) if self.extract_multiple else ([int(matches[0])] if matches else [])

        data[self.output_column] = data[self.columns].apply(
            lambda row: [id for col in self.columns for id in extract_ids(row[col])], axis=1
        )
        self.logger.info("Sample extracted reply IDs: %s", sample_matches)
        self.logger.info("ReplyReferenceExtractor completed. Output column: %s", self.output_column)
        return data
