from preprocessing.base import Preprocessor
import pandas as pd
from collections import defaultdict
from typing import List, Optional
from logs.logger import get_logger

logger = get_logger(__name__)

class ReplyGraphBuilder(Preprocessor):
    def __init__(self, columns: Optional[List[str]] = None):
        """
        Initialize the ReplyGraphBuilder preprocessor.

        :param columns: List of columns to process. If None, defaults to all columns.
        """
        self.columns = columns
        self.logger = get_logger(self.__class__.__name__)

    def fit(self, X):
        return self

    def transform(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Build reply graph by adding ParentCommentIds and ChildCommentIds columns.

        :param data: Input DataFrame containing CommentId and ReplyToIds columns.
        :return: Updated DataFrame with ParentCommentIds and ChildCommentIds columns.
        """
        logger.info("Running ReplyGraphBuilder")

        if 'CommentID' not in data.columns or 'ReplyToIDs' not in data.columns:
            self.logger.error(f"Input DataFrame must contain 'CommentID' and 'ReplyToIDs' columns. Instead found {data.columns.tolist()}")
            raise ValueError("Input DataFrame must contain 'CommentID' and 'ReplyToIDs' columns.")

        # Normalize ParentCommentIds
        data['ParentCommentIDs'] = data['ReplyToIDs'].apply(lambda x: x if isinstance(x, list) else [])

        # Build ChildCommentIDs using an efficient lookup
        child_map = defaultdict(list)
        for idx, row in data.iterrows():
            for parent_id in row['ParentCommentIDs']:
                child_map[parent_id].append(row['CommentID'])

        data['ChildCommentIDs'] = data['CommentID'].apply(lambda cid: child_map.get(cid, []))
        
        
        self.logger.info(f"ReplyGraphBuilder head {data.head()}")
        self.logger.info("ReplyGraphBuilder completed successfully")

        # count how many records are populated in ParentCommentIds and ChildCommentIds
        parent_count = data['ParentCommentIDs'].apply(lambda x: len(x) if isinstance(x, list) else 0).sum()
        child_count = data['ChildCommentIDs'].apply(lambda x: len(x) if isinstance(x, list) else 0).sum()
        self.logger.info(f"Total ParentCommentIDs populated: {parent_count}")
        self.logger.info(f"Total ChildCommentIDs populated: {child_count}")
        return data
