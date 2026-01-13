# fetchers/steam_store_fetcher.py
from .base import Fetcher
from typing import List
from logs.logger import get_logger
import os
import json
from utils.steam_http import steam_get

class SteamStoreFetcher(Fetcher):
    """Fetch Steam Store metadata for a list of app IDs."""
    def __init__(self, context, batch_size: int = 50):
        super().__init__(context)
        self.logger = get_logger(self.__class__.__name__)
        self.batch_size = batch_size
        self.logger.info("Initialized SteamStoreFetcher")

    def fetch(self, app_ids: List[int], cc="ie", l="en") -> list:
        results = []

        for i, app_id in enumerate(app_ids):
            self.logger.info(f"Fetching store data for AppID {app_id}")

            response = steam_get(
                "https://store.steampowered.com/api/appdetails",
                params={"appids": app_id, "cc": cc, "l": l},
                timeout=self.context.timeout
            )
            if not response:
                self.logger.warning(f"Failed to fetch data for AppID {app_id}")
                continue

            payload = response.json().get(str(app_id), {})
            if not payload.get("success"):
                continue

            data = payload["data"]

            if i == 0:
                os.makedirs("debug", exist_ok=True)
                with open("debug/steam_store_payload_example.json", "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)

            results.append(data)

        self.logger.info(f"Fetched store data for {len(results)} apps")
        return results



