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

        # sort the data by DiscussionID, CommentDateInserted, and CommentID to ensure consistent processing order
        data = data.sort_values(by=["DiscussionID", "CommentDateInserted", "CommentID"]).reset_index(drop=True)

        # Add canonical parent
        # The first ReplyToID becomes the parent used for chain construction
        data["CanonicalParentCommentID"] = data["ParentCommentIDs"].apply(
            lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None
        )

        # Add conversation flag
        data["IsConversationChain"] = data.apply(
            lambda row: (
                    (isinstance(row["ParentCommentIDs"], list)
                     and len(row["ParentCommentIDs"]) > 0)
                    or
                    (isinstance(row["ChildCommentIDs"], list)
                     and len(row["ChildCommentIDs"]) > 0)
            ),
            axis=1
        )

        # Initialize chain-related columns
        data["ChainID"] = None
        data["ChainDepth"] = None
        data["RootCommentID"] = None


        # Track chains
        chain_id = 0
        chain_map = {}

        for index, row in data.iterrows():
            comment_id = row["CommentID"]
            parent_id = row["CanonicalParentCommentID"]

            if not parent_id:  # No parent, start a new chain
                chain_id += 1
                data.at[index, "ChainID"] = chain_id
                data.at[index, "ChainDepth"] = 0
                data.at[index, "RootCommentID"] = comment_id
                chain_map[comment_id] = (chain_id, 0, comment_id)
            elif parent_id in chain_map:
                # Continue existing chain
                parent_chain, parent_depth, root_comment = chain_map[parent_id]

                data.at[index, "ChainID"] = parent_chain
                data.at[index, "ChainDepth"] = parent_depth + 1
                data.at[index, "RootCommentID"] = root_comment

                chain_map[comment_id] = (
                    parent_chain,
                    parent_depth + 1,
                    root_comment
                )

            else:
                # Parent exists in ReplyToIDs but was not processed
                # (deleted comment, filtered comment, missing data, etc.)
                chain_id += 1

                data.at[index, "ChainID"] = chain_id
                data.at[index, "ChainDepth"] = 0
                data.at[index, "RootCommentID"] = comment_id

                chain_map[comment_id] = (
                    chain_id,
                    0,
                    comment_id
                )

        self.logger.info(f"ReplyChainIdentifier dataframe columns: {data.columns.tolist()}")
        self.logger.info("ReplyChainIdentifier preprocessor completed.")
        return data
