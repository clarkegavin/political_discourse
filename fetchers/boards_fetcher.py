from datetime import datetime, timedelta
import time
from typing import List, Dict, Optional
import requests
from logs.logger import get_logger
from .base import Fetcher
import random

REQUEST_DELAY = 1.0  # seconds
BASE_URL = "https://www.boards.ie/api/v2/discussions"


class BoardsFetcher(Fetcher):
    """Fetcher for boards.ie discussions API."""

    def __init__(self, context):
        super().__init__(context)
        self.logger = get_logger(self.__class__.__name__)

    def fetch_batches(
        self,
        category_id: int,
        limit: int = 50,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_retries: int = 5,
        backoff_base: float = 2.0,
    ):
        """Generator yielding batches of discussions from the API."""
        if start_date and end_date:
            start = datetime.fromisoformat(start_date)
            end = datetime.fromisoformat(end_date)
            dates = [
                (start + timedelta(days=i)).date().isoformat()
                for i in range((end - start).days + 1)
            ]
        elif start_date:
            dates = [start_date]
        else:
            dates = [None]

        for date in dates:
            page = 1
            self.logger.info(f"Fetching discussions for dateLastComment={date}")

            while True:
                params = {
                    "CategoryID": category_id,
                    "limit": limit,
                    "page": page,
                }
                if date:
                    params["dateLastComment"] = date

                attempt = 0
                while True:
                    try:
                        resp = requests.get(
                            BASE_URL,
                            params=params,
                            timeout=self.context.timeout,
                            headers=self.context.headers,
                        )
                    except requests.RequestException as exc:
                        attempt += 1
                        if attempt > max_retries:
                            self.logger.error("Request failed after %d attempts: %s", attempt, exc)
                            raise
                        sleep_sec = (backoff_base ** attempt) + random.uniform(0, 1)
                        self.logger.warning(
                            "Request error: %s. Retrying in %.1f s (attempt %d/%d)",
                            exc,
                            sleep_sec,
                            attempt,
                            max_retries,
                        )
                        time.sleep(sleep_sec)
                        continue

                    if resp.status_code == 429:
                        attempt += 1
                        if attempt > max_retries:
                            self.logger.error("Rate limited after %d attempts", attempt)
                            resp.raise_for_status()
                        sleep_sec = (backoff_base ** attempt) + random.uniform(0, 1)
                        self.logger.warning(
                            "Rate limited – backing off %.1f s (attempt %d/%d)",
                            sleep_sec,
                            attempt,
                            max_retries,
                        )
                        time.sleep(sleep_sec)
                        continue

                    resp.raise_for_status()
                    break

                batch = resp.json()
                time.sleep(REQUEST_DELAY)

                if not batch:
                    break

                yield batch

                if len(batch) < limit:
                    break
                page += 1

    def fetch(
        self,
        category_id: int,
        limit: int = 50,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict]:
        """Implement Fetcher interface: fetch all results (non-streaming)."""
        all_results = []
        for batch in self.fetch_batches(category_id, limit, start_date, end_date):
            all_results.extend(batch)
        return all_results

