#preprocessing/analytical_document_assembler.py
import pandas as pd
from sqlalchemy.dialects.mssql.information_schema import columns

from preprocessing.base import Preprocessor
from typing import Optional, List
from logs.logger import get_logger



class AnalyticalDocumentAssembler(Preprocessor):
    def __init__(
        self,
        include_title: bool = True,
        include_opening_post: bool = True,
        include_comment_separators: bool = False,
        separator: str = "\n\n",
        document_text_column: str = "DocumentText",
    ):

        self.include_title = include_title
        self.include_opening_post = include_opening_post
        self.include_comment_separators = include_comment_separators
        self.separator = separator
        self.document_text_column = document_text_column
        self.logger = get_logger(self.__class__.__name__)

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None):
        # Validate required columns
        required_columns = [
            "DiscussionTitle", "DiscussionBody", "CommentRecords"
        ]
        missing_columns = [col for col in required_columns if col not in X.columns]
        if missing_columns:
            raise ValueError(f"AnalyticalDocumentAssembler - Missing required columns: {missing_columns}")
        return self

    def _assemble_comment_text(self, comment_records: List[dict]) -> str:
        """
        Helper method to assemble comment text from CommentRecords.
        """
        comments = [record["CommentBody"] for record in comment_records if record.get("CommentBody")]
        return self.separator.join(comments)

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Assembling analytical documents.")
        self.logger.info(f"AnalyticalDocumentAssembler available columns: {X.columns.tolist()}")

        def assemble_document(row):
            parts = []
            if self.include_title and pd.notnull(row["DiscussionTitle"]):
                parts.append(row["DiscussionTitle"])
            if self.include_opening_post and pd.notnull(row["DiscussionBody"]):
                parts.append(row["DiscussionBody"])
            comment_text = self._assemble_comment_text(row["CommentRecords"])
            parts.append(comment_text)
            return self.separator.join(parts)

        X[self.document_text_column] = X.apply(assemble_document, axis=1)
        X["DocumentWordCount"] = X[self.document_text_column].str.split().str.len()
        X["DocumentCharacterCount"] = X[self.document_text_column].str.len()
        #rename documentid
        X.rename(columns={"DocumentID": "DocumentDiscussionChainPart"}, inplace=True)

        # Create unique analytical document identifier
        X.insert(
            0,
            "DocumentID",
            range(1, len(X) + 1)
        )

        self.logger.info("Analytical documents assembled with the following columns: %s", X.columns.tolist())
        self.logger.info("Analytical documents assembled successfully.")

        return X

    def get_params(self, deep: bool = True):
        return {
            "include_title": self.include_title,
            "include_opening_post": self.include_opening_post,
            "include_comment_separators": self.include_comment_separators,
            "separator": self.separator,
            "document_text_column": self.document_text_column,
        }
