# pipelines/boards_comments_pipeline.py
from fetchers.factory import FetcherFactory
from data.data_source.factory import DiscussionSourceFactory
from data.sqlalchemy_connector import SQLAlchemyConnector
from data.extractors.boards_comments_extractor import BoardsCommentsExtractor
from data.models.boards_comment import BoardsComment
from logs.logger import get_logger
from typing import Optional

class BoardsCommentsPipeline:
    """Pipeline to fetch Boards.ie comments for discussions and save to DB."""

    def __init__(
        self,
        discussion_source_cfg: dict,
        limit: int = 500,
        date_start: Optional[str] = None,
        date_end: Optional[str] = None,
        connector: Optional[SQLAlchemyConnector] = None,
        chunk_size: int = 500,
        fetcher_name: str = "boards_comments",
    ):
        self.logger = get_logger(self.__class__.__name__)
        self.limit = limit
        self.date_start = date_start
        self.date_end = date_end
        self.chunk_size = chunk_size

        self.connector = connector or SQLAlchemyConnector()
        self.connector.create_tables(base=BoardsComment.__base__)

        # Create discussion source (db or api) to obtain discussion ids
        # create a discussion fetcher (boards) and pass it into the discussion source factory
        from fetchers.context import FetcherContext
        discussion_fetcher = FetcherFactory.create("boards", context=FetcherContext())
        self.discussion_source = DiscussionSourceFactory.create(discussion_source_cfg, connector=self.connector, fetcher=discussion_fetcher)

        # Fetcher for comments
        self.fetcher = FetcherFactory.create(fetcher_name, context=FetcherContext())

        self.extractor = BoardsCommentsExtractor(connector=self.connector, chunk_size=chunk_size)

    @classmethod
    def from_config(cls, cfg: dict):
        params = cfg.get("params", {})
        return cls(**params)

    def execute(self, data=None):
        # Get discussions to fetch comments for
        df = self.discussion_source.get_discussions()
        if df is None or df.empty:
            self.logger.info("No discussions to fetch comments for.")
            return df

        discussion_ids = df["DiscussionId"].astype(int).tolist()

        for discussion_id in discussion_ids:
            self.logger.info(f"Fetching comments for discussion {discussion_id}")

            # get existing commentIDs to avoid duplicates
            session = self.connector.get_session()
            try:
                existing = {row[0] for row in session.query(BoardsComment.commentID).filter_by(discussionID=discussion_id).all()}
            finally:
                session.close()

            # fetch all comments for this discussion (fetcher.fetch returns list)
            all_comments = self.fetcher.fetch(discussion_id, limit=self.limit, date_start=self.date_start, date_end=self.date_end)
            if not all_comments:
                self.logger.info("No comments returned for discussion %s", discussion_id)
                continue

            # filter out already existing comments
            new_records = [r for r in all_comments if r.get("commentID") not in existing]
            if not new_records:
                self.logger.info("No new comments for discussion %s", discussion_id)
                continue

            # save in chunks via extractor
            for i in range(0, len(new_records), self.extractor.chunk_size):
                chunk = new_records[i:i + self.extractor.chunk_size]
                self.extractor.save_data(chunk)
                self.logger.info(f"Saved chunk of {len(chunk)} comments for discussion {discussion_id}")
                for r in chunk:
                    existing.add(r.get("commentID"))

        return df
