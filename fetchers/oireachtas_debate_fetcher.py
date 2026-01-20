# fetchers/oireachtas_debate_fetcher.py
import requests
from typing import List, Dict
from logs.logger import get_logger
from .base import Fetcher

BASE_URL = "https://api.oireachtas.ie/v1/debates"


class OireachtasDebateFetcher(Fetcher):
    def __init__(self, context):
        super().__init__(context)
        self.logger = get_logger(self.__class__.__name__)

    def fetch(self, date: str, limit: int = 50) -> List[Dict]:
        results = []
        skip = 0
        total_expected = None

        while True:
            params = {
                "date": date,
                "limit": limit,
                "skip": skip,
            }

            resp = requests.get(
                BASE_URL,
                params=params,
                timeout=self.context.timeout,
                headers=self.context.headers,
            )

            if resp.status_code == 500:
                self.logger.warning(f"500 error at skip={skip}, stopping debates fetch")
                break

            resp.raise_for_status()
            data = resp.json()

            # --- read total count ONCE ---
            if total_expected is None:
                total_expected = data.get("head", {}).get("counts", {}).get("debateCount", 0)
                self.logger.info(
                    f"Date={date}: debateCount reported by API = {total_expected}"
                )

                if total_expected == 0:
                    self.logger.info(f"No debates available for {date}")
                    break

            batch = data.get("results", [])

            # --- hard stop on empty page ---
            if not batch:
                self.logger.info(
                    f"Empty results page at skip={skip}; stopping pagination"
                )
                break

            # sample logging
            ids = [r.get("debateRecord", {}).get("uri") for r in batch[:3]]
            self.logger.debug(f"Sample debate URIs at skip={skip}: {ids}")

            results.extend(batch)
            skip += limit

            # --- authoritative stop condition ---
            if skip >= total_expected:
                self.logger.info(
                    f"Reached debateCount ({total_expected}); stopping pagination"
                )
                break

        self.logger.info(
            f"Fetched {len(results)} debate records for {date} (expected {total_expected})"
        )

        # sanity check (important for thesis)
        if total_expected is not None and len(results) != total_expected:
            self.logger.warning(
                f"Mismatch for {date}: expected {total_expected}, got {len(results)}"
            )

        return results
