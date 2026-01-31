#data/data_source/db_discussion_source.py
import pandas as pd
from .base import DiscussionSource
from logs.logger import get_logger

class DBDiscussionSource(DiscussionSource):

    def __init__(
        self,
        connector,
        table: str = "dbo.boards_discussions",
        where: str | None = None,
    ):
        self.logger = get_logger(self.__class__.__name__)
        self.connector = connector
        self.table = table
        self.where = where

    def get_discussions(self) -> pd.DataFrame:
        self.logger.info(f"Fetching discussions from DB table {self.table}")
        query = f"SELECT DiscussionId FROM {self.table}"
        if self.where:
            query += f" WHERE {self.where}"

        session = self.connector.get_session()
        try:
            df = pd.read_sql(query, session.bind)
            return df
        finally:
            session.close()

