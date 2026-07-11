# pipelines/boards_document_pipeline.py
from typing import Optional
import os
import pandas as pd

from data import ExtractorFactory
from data.extractor import DataExtractor
from logs.logger import get_logger
from pipelines.base import Pipeline


class BoardsDocumentPipeline(Pipeline):
    """
    ETL pipeline to extract boards discussion and comments, aggregate the data and output a new table
    """

    def __init__(self, discussion_extractor: DataExtractor, comment_extractor: DataExtractor, inactivity_threshold_days: int = 7):
        self.logger = get_logger(self.__class__.__name__)
        self.discussion_extractor = discussion_extractor
        self.comment_extractor = comment_extractor
        self.df: Optional[pd.DataFrame] = None
        self.discussions = None
        self.comments = None
        self.inactivity_threshold_days = inactivity_threshold_days

    @classmethod
    def from_config(cls, cfg: dict):
        params = cfg.get("params", {}).copy()

        extractor_type = cfg.get("extractor_type")

        if extractor_type != "table":
            raise ValueError(
                f"Unsupported extractor type '{extractor_type}'"
            )

        discussion_params = params.pop(
            "discussion_extractor_params"
        )

        comment_params = params.pop(
            "comment_extractor_params"
        )

        discussion_model = discussion_params.pop("model")
        comment_model = comment_params.pop("model")

        discussion_extractor = (
            ExtractorFactory.create_table_extractor(
                model=discussion_model,
                **discussion_params
            )
        )

        comment_extractor = (
            ExtractorFactory.create_table_extractor(
                model=comment_model,
                **comment_params
            )
        )

        return cls(
            discussion_extractor=discussion_extractor,
            comment_extractor=comment_extractor,
            **params
        )


    def extract(self) -> None:
        """Extract data using the configured DataExtractor."""
        self.logger.info("Extracting discussions")
        discussions = pd.DataFrame(
            self.discussion_extractor.fetch_all()
        )

        self.logger.info("Extracting comments")
        comments = pd.DataFrame(
            self.comment_extractor.fetch_all()
        )

        self.discussions = discussions
        self.comments = comments

    def transform(self):
        self.logger.info(
            f"Aggregating discussions using "
            f"{self.inactivity_threshold_days}-day inactivity threshold"
        )

        comments = self.comments.copy()
        discussions = self.discussions.copy()

        # Ensure datetime
        comments["dateInserted"] = pd.to_datetime(
            comments["dateInserted"]
        )

        discussions["DateLastComment"] = pd.to_datetime(
            discussions["DateLastComment"]
        )

        # Merge comments with discussion metadata
        merged = comments.merge(
            discussions[
                [
                    "DiscussionId",
                    "Type",
                    "Title",
                    "Body",
                    "CategoryId",
                    "DateLastComment"
                ]
            ],
            left_on="discussionID",
            right_on="DiscussionId",
            how="inner"
        )

        # Sort chronologically within each discussion
        merged = (
            merged
            .sort_values(
                [
                    "DiscussionId",
                    "dateInserted"
                ]
            )
        )

        # ---------------------------------------------------------
        # Create active discussion periods
        #
        # A new document begins when there is a gap greater than
        # inactivity_threshold_days
        # ---------------------------------------------------------

        merged["PreviousCommentDate"] = (
            merged
            .groupby("DiscussionId")["dateInserted"]
            .shift(1)
        )

        merged["DaysSincePreviousComment"] = (
            (
                    merged["dateInserted"]
                    -
                    merged["PreviousCommentDate"]
            )
            .dt.days
        )

        merged["NewDiscussionPeriod"] = (
            merged["DaysSincePreviousComment"]
            .gt(self.inactivity_threshold_days)
            .fillna(True)
        )

        # Create period identifier
        merged["DiscussionPeriodId"] = (
            merged
            .groupby("DiscussionId")["NewDiscussionPeriod"]
            .cumsum()
        )

        # ---------------------------------------------------------
        # Aggregate active periods into documents
        # ---------------------------------------------------------

        documents = (
            merged
            .groupby(
                [
                    "DiscussionId",
                    "DiscussionPeriodId",
                    "Type",
                    "Title",
                    "Body",
                    "CategoryId",
                    "DateLastComment"
                ],
                as_index=False
            )
            .agg(
                CommentCount=(
                    "commentID",
                    "count"
                ),
                Comments=(
                    "body",
                    "\n\n".join
                ),
                PostDate=(
                    "dateInserted",
                    "min"
                )
            )
        )

        # ---------------------------------------------------------
        # Create final document text
        # ---------------------------------------------------------

        documents["Document"] = (
                documents["Title"].fillna("")
                + "\n\n"
                + documents["Body"].fillna("")
                + "\n\n"
                + documents["Comments"].fillna("")
        )

        # ---------------------------------------------------------
        # Add required fields only
        # ---------------------------------------------------------

        documents["OpeningPost"] = documents["Body"]

        documents["PostYear"] = (
            documents["PostDate"]
            .dt.year
        )

        documents["PostMonth"] = (
            documents["PostDate"]
            .dt.month
        )

        # Add auto-increment DocumentId
        documents.insert(
            0,
            "DocumentId",
            range(1, len(documents) + 1)
        )

        self.df = documents[
            [
                "DocumentId",
                "DiscussionId",
                "Type",
                "Title",
                "OpeningPost",
                "CategoryId",
                "PostYear",
                "PostMonth",
                "CommentCount",
                "DateLastComment",
                "Document"
            ]
        ]
    # def load(self):
    #     self.logger.info("Loading data into the database")
    #     self.df.to_sql(
    #         "boards_conceptual_model",
    #         con=self.engine,
    #         schema="dbo",
    #         if_exists="append",
    #         index=False
    #     )

    def execute(self, data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Execute the full ETL process and return the resulting DataFrame.
        `data` parameter is ignored here, since extraction starts from scratch.
        """
        self.logger.info("Starting BoardsDocumentPipeline execution")
        self.extract()
        self.transform()
        #self.load()
        self.logger.info("Pipeline execution complete")
        return self.df
