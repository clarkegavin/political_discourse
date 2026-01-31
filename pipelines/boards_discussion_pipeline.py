# pipelines/boards_discussion_pipeline.py
from fetchers.factory import FetcherFactory
from fetchers.context import FetcherContext
from data.sqlalchemy_connector import SQLAlchemyConnector
from data.extractors.boards_discussion_extractor import BoardsDiscussionExtractor
from data.models.boards_discussion import BoardsDiscussion
from logs.logger import get_logger
from typing import Optional


class BoardsDataPipeline:
    """Pipeline to fetch Boards.ie discussions and save as snapshot in DB."""

    def __init__(
        self,
        category_id: int,
        limit: int = 50,
        api_key: Optional[str] = None,
        date_start: Optional[str] = None,
        date_end: Optional[str] = None,
        connector: Optional[SQLAlchemyConnector] = None,
        chunk_size: int = 500,
    ):
        self.logger = get_logger(self.__class__.__name__)
        self.category_id = category_id
        self.limit = limit
        self.date_start = date_start
        self.date_end = date_end

        self.connector = connector or SQLAlchemyConnector()
        # Ensure tables exist
        self.connector.create_tables(base=BoardsDiscussion.__base__)

        # Fetcher for API
        self.context = FetcherContext(api_key=api_key)
        self.fetcher = FetcherFactory.create("boards", context=self.context)

        # Extractor for DB
        self.extractor = BoardsDiscussionExtractor(connector=self.connector, chunk_size=chunk_size)

    @classmethod
    def from_config(cls, cfg: dict):
        params = cfg.get("params", {})
        return cls(**params)

    def execute(self, data=None):
        if not self.category_id:
            raise ValueError("category_id must be set for BoardsDataPipeline")

        self.logger.info(f"Starting Boards snapshot pipeline for category {self.category_id}")

        # --- Fetch and save in batches ---
        batch_number = 0
        for batch in self.fetch_batches():
            batch_number += 1
            self.logger.info(f"Processing batch #{batch_number} ({len(batch)} discussions)")
            self.extractor.save_data(batch)

        self.logger.info("Boards snapshot pipeline completed.")

    def fetch_batches(self):
        """Generator to fetch batches from the API per page/day."""
        records = self.fetcher.fetch(
            category_id=self.category_id,
            limit=self.limit,
            start_date=self.date_start,
            end_date=self.date_end,
        )
        if not records:
            return []

        # Yield batches for chunked DB inserts
        for i in range(0, len(records), self.extractor.chunk_size):
            yield records[i:i + self.extractor.chunk_size]
