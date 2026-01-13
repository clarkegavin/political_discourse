# fetchers/steam_review_fetcher.py
from .base import Fetcher
from typing import List
from logs.logger import get_logger
import os
import json
from utils.steam_http import steam_get


class SteamReviewFetcher(Fetcher):
    """Fetch Steam review stats."""
    def __init__(self, context):
        super().__init__(context)
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("Initialized SteamReviewFetcher")

    def fetch(self, app_ids: List[int], cc="ie", language="english") -> list:
        results = []

        for i, app_id in enumerate(app_ids):
            self.logger.info(f"Fetching review summary for AppID {app_id}")
            url = f"https://store.steampowered.com/appreviews/{app_id}"
            params = {
                "json": 1,
                "language": language,
                "purchase_type": "all",
                "num_per_page": 1
            }

            try:
                response = steam_get(url, params=params, timeout=self.context.timeout)

                if response is None:
                    self.logger.warning(f"Failed to fetch reviews for AppID {app_id}")
                    continue
                payload = response.json()
            except Exception as e:
                self.logger.warning(f"Exception fetching reviews for AppID {app_id}: {e}")
                continue


            summary = payload.get("query_summary")
            if not summary:
                continue

            if i == 0:
                os.makedirs("debug", exist_ok=True)
                with open("debug/steam_review_payload_example.json", "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2, ensure_ascii=False)


            results.append({
                "AppID": app_id,
                "review_score": summary.get("review_score"),
                "review_score_desc": summary.get("review_score_desc"),
                "total_reviews": summary.get("total_reviews"),
                "total_positive": summary.get("total_positive"),
                "total_negative": summary.get("total_negative"),
            })

        self.logger.info(f"Fetched review summaries for {len(results)} apps")
        return results
