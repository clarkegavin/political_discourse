#data/data_source/api_discussion_source.py
import pandas as pd
from .base import DiscussionSource
from logs.logger import get_logger

class APIDiscussionSource(DiscussionSource):

    def __init__(self, fetcher, category_id: int, limit: int = 100):
        self.logger = get_logger(self.__class__.__name__)
        self.fetcher = fetcher
        self.category_id = category_id
        self.limit = limit

    def get_discussions(self) -> pd.DataFrame:
        self.logger.info(
            f"Fetching discussions from API for category {self.category_id}"
        )
        records = self.fetcher.fetch(
            category_id=self.category_id,
            limit=self.limit,
        )
        return pd.DataFrame(records)[["discussionID"]].rename(
            columns={"discussionID": "DiscussionId"}
        )
