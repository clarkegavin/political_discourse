# pipelines/steam_pipeline.py
from data.mapper import map_payload_to_steamgame
from data.models import SteamGame
from fetchers.factory import FetcherFactory
from data.factory import ExtractorFactory
from data.sqlalchemy_connector import SQLAlchemyConnector
from typing import Optional, List
from logs.logger import get_logger
from fetchers.context import FetcherContext
BATCH_SIZE = 100  # store fetch batch size
class SteamDataPipeline:
    """End-to-end pipeline for fetching and saving Steam API data."""


    def __init__(
        self,
        connector: Optional[SQLAlchemyConnector] = None,
        chunk_size: int = 100,
        limit_apps: Optional[int] = None,
        api_key: Optional[str] = None,
        offset_apps: int = 0
    ):
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("Initializing SteamDataPipeline")
        self.connector = connector or SQLAlchemyConnector()
        self.chunk_size = chunk_size
        self.limit_apps = limit_apps
        self.offset_apps = offset_apps
        self.connector.create_tables(base=SteamGame.__base__)


        # Create shared context
        self.fetcher_context = FetcherContext(api_key=api_key)

        # Create fetchers
        self.logger.info("Creating steam app list fetcher")
        self.app_list_fetcher = FetcherFactory.create("steam_app_list", context=self.fetcher_context)
        self.logger.info("Creating steam store fetcher")
        self.store_fetcher = FetcherFactory.create("steam_store", context=self.fetcher_context)
        self.logger.info("Creating steam review fetcher")
        self.review_fetcher = FetcherFactory.create("steam_review", context=self.fetcher_context)
        self.logger.info("Creating steam current user fetcher")
        self.current_user_fetcher = FetcherFactory.create("steam_current_user", context=self.fetcher_context)

        # Create extractor
        self.logger.info("Creating steam extractor")
        self.steam_extractor = ExtractorFactory.create_steam_extractor(
            connector=self.connector,
            chunk_size=self.chunk_size,
            )

    def run(self):
        app_ids = list(self.app_list_fetcher.fetch().keys())
        self.logger.info(f"Fetched {len(app_ids)} Steam app IDs")

        if self.offset_apps:
            app_ids = app_ids[self.offset_apps:]
            self.logger.info(f"Offsetting app IDs by {self.offset_apps}, {len(app_ids)} remaining")

        if self.limit_apps:
            app_ids = app_ids[:self.limit_apps]

        for i in range(0, len(app_ids), BATCH_SIZE):
            batch = app_ids[i:i + BATCH_SIZE]
            self.logger.info(f"Processing apps {i}–{i + len(batch)}")

            store_data = self.store_fetcher.fetch(batch)
            review_data = self.review_fetcher.fetch(batch)
            current_user_data = self.current_user_fetcher.fetch(batch)

            review_lookup = {r["AppID"]: r for r in review_data}
            current_user_lookup = {u["AppID"]: u for u in current_user_data}

            mapped = []

            for app in store_data:
                app_id = app.get("steam_appid")
                if app_id in review_lookup:
                    app.update(review_lookup[app_id])
                if app_id in current_user_lookup:
                    app.update(current_user_lookup[app_id])

                mapped.append(map_payload_to_steamgame(app))

            self.steam_extractor.save_data(mapped)
            self.logger.info(f"Saved {len(mapped)} apps to database")

        self.logger.info("Steam data pipeline completed successfully")
