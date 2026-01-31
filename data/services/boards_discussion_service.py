# data/services/boards_discussion_service.py
from typing import Optional, List
import pandas as pd

class BoardsDiscussionIngestionService:

    def __init__(self, fetcher, extractor, chunk_size=500):
        self.fetcher = fetcher
        self.extractor = extractor
        self.chunk_size = chunk_size
        self.logger = fetcher.logger

    def ingest(
            self,
            category_id: int,
            limit: int = 50,
            date_start: Optional[str] = None,
            date_end: Optional[str] = None,
    ):
        self.logger.info(
            f"Ingesting Boards discussions: "
            f"category={category_id}, {date_start} → {date_end}"
        )

        # fetcher returns generator of batches instead of all records at once
        for batch in self.fetcher.fetch_batches(
                category_id=category_id,
                limit=limit,
                start_date=date_start,
                end_date=date_end,
        ):
            if not batch:
                continue
            self.extractor.save_data(batch)
            self.logger.info(f"Saved batch of {len(batch)} discussions")

    def _save_in_chunks(self, records: List[dict]):
        self.logger.info(
            f"Saving {len(records)} discussions "
            f"in chunks of {self.chunk_size}"
        )

        for i in range(0, len(records), self.chunk_size):
            chunk = records[i:i + self.chunk_size]
            self.extractor.save_data(chunk)
            self.logger.info(f"Saved chunk of {len(chunk)} discussions")
