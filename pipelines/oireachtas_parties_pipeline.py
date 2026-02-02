# pipelines/oireachtas_parties_pipeline.py
from fetchers.factory import FetcherFactory
from fetchers.context import FetcherContext
from data.sqlalchemy_connector import SQLAlchemyConnector
from data.extractors.oireachtas_party_extractor import OireachtasPartyExtractor
from data.models.oireachtas_party import OireachtasParty
from logs.logger import get_logger
from typing import Optional


class OireachtasPartiesPipeline:
    """Pipeline to fetch Oireachtas parties and persist to dbo.oireachtas_parties."""

    def __init__(
        self,
        chamber: str = "dail",
        house_no: int = 34,
        limit: int = 50,
        request_delay: float = 1.0,
        api_key: Optional[str] = None,
        connector: Optional[SQLAlchemyConnector] = None,
        chunk_size: int = 500,
    ):
        self.logger = get_logger(self.__class__.__name__)
        self.chamber = chamber
        self.house_no = house_no
        self.limit = limit
        self.request_delay = request_delay

        self.connector = connector or SQLAlchemyConnector()
        # Ensure the table exists
        self.connector.create_tables(base=OireachtasParty.__base__)

        self.context = FetcherContext(api_key=api_key)
        self.fetcher = FetcherFactory.create("oireachtas_parties", context=self.context)

        self.extractor = OireachtasPartyExtractor(connector=self.connector, chunk_size=chunk_size)

    @classmethod
    def from_config(cls, cfg: dict):
        params = cfg.get("params", {})
        return cls(**params)

    def execute(self, data=None):
        self.logger.info(f"Starting Oireachtas parties pipeline: chamber={self.chamber} house_no={self.house_no}")

        batch_number = 0
        # Use fetch_batches if fetcher provides it (streaming); otherwise fall back to fetch()
        fetch_batches = getattr(self.fetcher, "fetch_batches", None)
        if callable(fetch_batches):
            iterator = fetch_batches(
                chamber=self.chamber,
                house_no=self.house_no,
                limit=self.limit,
                request_delay=self.request_delay,
            )
        else:
            # fallback (non-streaming): fetch everything then iterate in one batch
            all_records = self.fetcher.fetch(
                chamber=self.chamber,
                house_no=self.house_no,
                limit=self.limit,
                request_delay=self.request_delay,
            )
            iterator = (all_records[i:i + self.extractor.chunk_size] for i in range(0, len(all_records), self.extractor.chunk_size))

        for batch in iterator:
            batch_number += 1
            self.logger.info(f"Processing batch #{batch_number} ({len(batch)} records)")
            # map / filter fields early so extractor receives minimal payload
            cleaned = [self._map_record(r) for r in batch]
            self.extractor.save_data(cleaned)

        self.logger.info("Oireachtas parties pipeline completed")

    @staticmethod
    def _map_record(r: dict) -> dict:
        party = r.get("party") or r
        house = r.get("house") or party.get("house") or {}

        return {
            "party_show_as": party.get("showAs"),
            "party_code": party.get("partyCode"),
            "house_code": house.get("houseCode"),
            "house_no": house.get("houseNo"),
        }
