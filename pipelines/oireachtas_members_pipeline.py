# pipelines/oireachtas_members_pipeline.py
from fetchers.factory import FetcherFactory
from fetchers.context import FetcherContext
from data.sqlalchemy_connector import SQLAlchemyConnector
from data.extractors.oireachtas_member_extractor import OireachtasMemberExtractor
from data.models.oireachtas_member import OireachtasMember
from logs.logger import get_logger
from typing import Optional


class OireachtasMembersPipeline:
    """Pipeline to fetch Oireachtas members and normalize into relational tables."""

    def __init__(
        self,
        date_start: str,
        date_end: str,
        chamber: str = "dail",
        house_no: int = 34,
        limit: int = 50,
        request_delay: float = 1.0,
        api_key: Optional[str] = None,
        connector: Optional[SQLAlchemyConnector] = None,
        chunk_size: int = 100,
    ):
        self.logger = get_logger(self.__class__.__name__)
        self.date_start = date_start
        self.date_end = date_end
        self.chamber = chamber
        self.house_no = house_no
        self.limit = limit
        self.request_delay = request_delay

        self.connector = connector or SQLAlchemyConnector()
        # Ensure member tables exist
        self.connector.create_tables(base=OireachtasMember.__base__)

        self.context = FetcherContext(api_key=api_key)
        self.fetcher = FetcherFactory.create("oireachtas_members", context=self.context)

        self.extractor = OireachtasMemberExtractor(connector=self.connector, chunk_size=chunk_size)

    @classmethod
    def from_config(cls, cfg: dict):
        params = cfg.get("params", {})
        return cls(**params)

    def execute(self, data=None):
        if not self.date_start or not self.date_end:
            raise ValueError("date_start and date_end must be set for members pipeline")

        self.logger.info(f"Fetching Oireachtas members {self.date_start} → {self.date_end}")

        fetch_batches = getattr(self.fetcher, "fetch_batches", None)
        if callable(fetch_batches):
            iterator = fetch_batches(
                date_start=self.date_start,
                date_end=self.date_end,
                chamber=self.chamber,
                house_no=self.house_no,
                limit=self.limit,
                request_delay=self.request_delay,
            )
        else:
            all_records = self.fetcher.fetch(
                date_start=self.date_start,
                date_end=self.date_end,
                chamber=self.chamber,
                house_no=self.house_no,
                limit=self.limit,
                request_delay=self.request_delay,
            )
            iterator = (all_records[i:i + self.extractor.chunk_size] for i in range(0, len(all_records), self.extractor.chunk_size))

        batch_num = 0
        for batch in iterator:
            batch_num += 1
            self.logger.info(f"Processing member batch #{batch_num} ({len(batch)} records)")
            # Save normalized data (extractor handles normalization)
            self.extractor.save_data(batch)

        self.logger.info("Oireachtas members pipeline completed")

