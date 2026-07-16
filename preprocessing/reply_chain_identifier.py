import pandas as pd
from preprocessing.base import Preprocessor
from logs.logger import get_logger


class ReplyChainIdentifier(Preprocessor):
    """
    Preprocessor to identify conversational reply chains in a DataFrame.
    """

    def __init__(self):
        super().__init__()
        self.logger = get_logger(self.__class__.__name__)

    def fit(self, data: pd.DataFrame, **kwargs):
        """
        Fit method for compatibility. No fitting required for this preprocessor.
        """
        return self

    def transform(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        self.logger.info("Starting ReplyChainIdentifier preprocessor.")

        # Ensure required columns exist
        required_columns = ["CommentID", "ParentCommentIDs", "ChildCommentIDs"]
        for col in required_columns:
            if col not in data.columns:
                raise ValueError(f"Missing required column: {col}")

        # Initialize chain-related columns
        data["ChainID"] = None
        data["ChainDepth"] = None
        data["RootCommentID"] = None

        # Track chains
        chain_id = 0
        chain_map = {}

        for index, row in data.iterrows():
            comment_id = row["CommentID"]
            parent_ids = row["ParentCommentIDs"]

            if not parent_ids:  # No parent, start a new chain
                chain_id += 1
                data.at[index, "ChainID"] = chain_id
                data.at[index, "ChainDepth"] = 0
                data.at[index, "RootCommentID"] = comment_id
                chain_map[comment_id] = (chain_id, 0, comment_id)
            else:
                # Check if any parent is already part of a chain
                parent_chain = None
                parent_depth = None
                root_comment = None
                for parent_id in parent_ids:
                    if parent_id in chain_map:
                        parent_chain, parent_depth, root_comment = chain_map[parent_id]
                        break

                if parent_chain is not None:  # Continue the chain
                    data.at[index, "ChainID"] = parent_chain
                    data.at[index, "ChainDepth"] = parent_depth + 1
                    data.at[index, "RootCommentID"] = root_comment
                    chain_map[comment_id] = (parent_chain, parent_depth + 1, root_comment)
                else:  # Start a new chain
                    chain_id += 1
                    data.at[index, "ChainID"] = chain_id
                    data.at[index, "ChainDepth"] = 0
                    data.at[index, "RootCommentID"] = comment_id
                    chain_map[comment_id] = (chain_id, 0, comment_id)

        self.logger.info(f"ReplyChainIdentifier dataframe columns: {data.columns.tolist()}")
        self.logger.info("ReplyChainIdentifier preprocessor completed.")
        return data
