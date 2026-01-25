# pipelines/boards_pipeline.py
from fetchers.factory import FetcherFactory
from fetchers.context import FetcherContext
from logs.logger import get_logger
import pandas as pd
from typing import Optional


class BoardsDataPipeline:
    """Pipeline to fetch discussions from boards.ie and return a DataFrame.

    Config params (via pipelines.yaml):
      category_id: int (required)
      limit: int (optional, default 50)
      date_start: str (optional, ISO date)
      date_end: str (optional, ISO date)
      api_key: str (optional)

    The pipeline will paginate the API and return a pandas.DataFrame with
    columns: discussionID, name, categoryID, dateLastComment, countComments, canonicalUrl
    """

    def __init__(
        self,
        category_id: int,
        limit: int = 50,
        api_key: Optional[str] = None,
        date_start: Optional[str] = None,
        date_end: Optional[str] = None,
    ):
        self.logger = get_logger(self.__class__.__name__)
        self.category_id = category_id
        self.limit = limit or 50
        self.date_start = date_start
        self.date_end = date_end

        self.context = FetcherContext(api_key=api_key)
        self.fetcher = FetcherFactory.create("boards", context=self.context)

    @classmethod
    def from_config(cls, cfg: dict):
        params = cfg.get("params", {})
        return cls(**params)

    def execute(self, data=None) -> pd.DataFrame:
        if not self.category_id:
            raise ValueError("category_id must be set for BoardsDataPipeline")

        self.logger.info(f"Fetching boards discussions for category {self.category_id}")

        records = self.fetcher.fetch(
            category_id=self.category_id,
            limit=self.limit,
            start_date=self.date_start,
            end_date=self.date_end,
        )

        # normalize to required columns
        df = pd.DataFrame(records)
        if df.empty:
            self.logger.info("No records fetched from boards.ie")
            return df

        # Some records may use camelCase keys - ensure consistent naming
        mapping = {
            "discussionID": "discussionID",
            "name": "name",
            "categoryID": "categoryID",
            "dateLastComment": "dateLastComment",
            "countComments": "countComments",
            "canonicalUrl": "canonicalUrl",
        }

        # keep only the keys that exist
        cols = [k for k in mapping.keys() if k in df.columns]
        df = df[cols].rename(columns=mapping)

        # ensure expected columns exist in final df (add missing as NaN)
        for col in mapping.values():
            if col not in df.columns:
                df[col] = pd.NA

        # reorder
        df = df[list(mapping.values())]

        self.logger.info(f"Boards pipeline fetched {len(df)} records")
        return df

