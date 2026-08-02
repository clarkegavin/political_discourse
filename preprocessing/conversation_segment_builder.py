#preprocessing/conversation_segment_builder.py
from preprocessing.base import Preprocessor
from typing import List
import pandas as pd
from logs.logger import get_logger

class ConversationSegmentBuilder(Preprocessor):
    def __init__(self, strategy: str = "reply_chain"):
        """
        Initialize the ConversationSegmentBuilder.

        :param strategy: Strategy for segmenting conversations. Options: "chronological", "reply_chain", "hybrid", "discussion".
        """
        self.strategy = strategy
        self.logger = get_logger(self.__class__.__name__)

    def fit(self, data: pd.DataFrame, **kwargs):
        """
        Fit method for compatibility. No fitting required for this preprocessor.
        """
        return self

    def transform(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Execute the conversation segmentation process.

        :param data: Input DataFrame containing discussion data.
        :param kwargs: Additional parameters from YAML configuration.
        :return: DataFrame with conversation segments.
        """
        self.logger.info("Starting ConversationSegmentBuilder with strategy: %s", self.strategy)
        self.logger.info(f"ConversationSegmentBuilder available columns: {data.columns.tolist()}")

        # Validate required columns
        required_columns = ["DiscussionID", "CommentID", "ChainID", "DiscussionDateInserted", "CommentWordCount"]
        for col in required_columns:
            if col not in data.columns:
                raise ValueError(f"Missing required column: {col}")

        if self.strategy == "chronological":
            return self._segment_chronologically(data)
        elif self.strategy == "reply_chain":
            return self._segment_by_reply_chain(data)
        elif self.strategy == "hybrid":
            return self._segment_hybrid(data)
        elif self.strategy == "discussion":
            return self._segment_by_discussion(data)
        else:
            raise ValueError(f"Unknown strategy: {self.strategy}")

    def _segment_chronologically(self, data: pd.DataFrame) -> pd.DataFrame:
        """Segment conversations chronologically."""
        self.logger.info("Segmenting conversations chronologically.")
        grouped = data.groupby("DiscussionID")
        segments = []

        for discussion_id, group in grouped:
            group = group.sort_values(by="DiscussionDateInserted")
            segment = self._create_segment(group, "chronological")
            segments.append(segment)

        return pd.concat(segments, ignore_index=True)

    def _segment_by_reply_chain(self, data: pd.DataFrame) -> pd.DataFrame:
        """Segment conversations by reply chains."""
        self.logger.info("Segmenting conversations by reply chains.")
        grouped = data.groupby(["DiscussionID", "ChainID"])
        segments = []

        for (discussion_id, chain_id), group in grouped:
            segment = self._create_segment(group, "reply_chain")
            segments.append(segment)

        return pd.concat(segments, ignore_index=True)

    def _segment_hybrid(self, data: pd.DataFrame) -> pd.DataFrame:
        """Segment conversations using a hybrid strategy."""
        self.logger.info("Segmenting conversations using hybrid strategy.")
        segments = []

        for discussion_id, group in data.groupby("DiscussionID"):
            standalone_comments = group[~group["IsConversationChain"]]
            reply_chains = group[group["IsConversationChain"]]

            if not standalone_comments.empty:
                #self.logger.info("Creating standalone segment for DiscussionID=%s with %d comments", discussion_id, len(standalone_comments))
                standalone_segment = self._create_segment(standalone_comments, "chronological")
                segments.append(standalone_segment)

            for chain_id, chain_group in reply_chains.groupby("ChainID"):
                chain_segment = self._create_segment(chain_group, "reply_chain")
                segments.append(chain_segment)

        return pd.concat(segments, ignore_index=True)

    def _segment_by_discussion(self, data: pd.DataFrame) -> pd.DataFrame:
        """Segment entire discussions into single documents."""
        self.logger.info("Segmenting entire discussions.")
        grouped = data.groupby("DiscussionID")
        segments = []

        for discussion_id, group in grouped:
            segment = self._create_segment(group, "discussion")
            segments.append(segment)

        return pd.concat(segments, ignore_index=True)

    def _build_comment_records(self, group: pd.DataFrame) -> List[dict]:
        """
        Build a list of dictionaries representing each comment in the group.

        :param group: DataFrame containing comments for a single segment.
        :return: List of dictionaries with comment-level data.
        """
        group = group.sort_values(by="CommentDateInserted")
        comment_records = []

        for _, row in group.iterrows():
            comment_record = {
                "CommentID": row.get("CommentID"),
                "CommentDiscussionID": row.get("DiscussionID"),
                "CommentName": row.get("CommentName"),
                "CommentCategoryID": row.get("CommentCategoryID"),
                "CommentBody": row.get("CommentBody"),
                "CommentDateInserted": row.get("CommentDateInserted"),
                "CommentDateUpdated": row.get("CommentDateUpdated"),
                "CommentUpdateUserID": row.get("CommentUpdateUserID"),
                "CommentScore": row.get("CommentScore"),
                "CommentDepth": row.get("CommentDepth"),
                "CommentScoreChildComments": row.get("CommentScoreChildComments"),
                "CommentCountChildComments": row.get("CommentCountChildComments"),
                "CommentUrl": row.get("CommentUrl"),
                "ReplyToIDs": row.get("ReplyToIDs"),
                "ParentCommentIDs": row.get("ParentCommentIDs"),
                "ChildCommentIDs": row.get("ChildCommentIDs"),
                "CommentWordCount": row.get("CommentWordCount"),
                "CommentCharacterCount": row.get("CommentCharacterCount"),
                "CommentSentenceCount": row.get("CommentSentenceCount"),
                "ChainID": row.get("ChainID"),
                "ChainDepth": row.get("ChainDepth"),
                "RootCommentID": row.get("RootCommentID")
            }
            comment_records.append(comment_record)

        return comment_records

    def _create_segment(self, group: pd.DataFrame, document_type: str) -> pd.DataFrame:
        """Create a single conversation segment."""
        document = {
            "DocumentID": group["DiscussionID"].iloc[0],
            "DiscussionID": group["DiscussionID"].iloc[0],
            "DiscussionTitle": group["DiscussionTitle"].iloc[0] if "DiscussionTitle" in group.columns else None,
            "DiscussionBody": group["DiscussionBody"].iloc[0] if "DiscussionBody" in group.columns else None,
            "DiscussionCategoryID": group["DiscussionCategoryID"].iloc[0] if "DiscussionCategoryID" in group.columns else None,
            "DiscussionDateInserted": group["DiscussionDateInserted"].min(),
            "DocumentType": document_type,
            "DocumentStartDate": group["DiscussionDateInserted"].min(),
            "DocumentEndDate": group["DiscussionDateInserted"].max(),
            "ChainID": group["ChainID"].iloc[0] if "ChainID" in group.columns else None,
            "CommentIDs": group["CommentID"].tolist(),
            "CommentCount": len(group),
            "CommentWordCount": group["CommentWordCount"].sum(),
            "CommentCharacterCount": group["CommentCharacterCount"].sum(),
            "DurationHours": (group["DiscussionDateInserted"].max() - group["DiscussionDateInserted"].min()).total_seconds() / 3600,
            "InactivitySplits": False,  # Placeholder for future implementation
            "CommentRecords": self._build_comment_records(group), # New column for detailed comment-level data
            "IsConversationChain": group["IsConversationChain"].iloc[0] if "IsConversationChain" in group.columns else None
        }

        # self.logger.info(
        #     "Created %s segment | DiscussionID=%s | ChainID=%s | Comments=%d",
        #     document_type,
        #     group["DiscussionID"].iloc[0],
        #     group["ChainID"].iloc[0],
        #     len(group)
        # )
        return pd.DataFrame([document])
