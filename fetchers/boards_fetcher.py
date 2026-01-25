# fetchers/boards_fetcher.py
import time

import requests
from typing import List, Dict, Optional
from logs.logger import get_logger
from .base import Fetcher
from datetime import datetime, timedelta
REQUEST_DELAY = 1.0  # seconds

BASE_URL = "https://www.boards.ie/api/v2/discussions"


class BoardsFetcher(Fetcher):
    """Fetcher for boards.ie discussions API.

    It paginates by 'page' and uses 'limit' per page. It supports optional
    dateLastComment start and end bounds. Returns a flat list of discussion dicts.
    """

    def __init__(self, context):
        super().__init__(context)
        self.logger = get_logger(self.__class__.__name__)

    def fetch(
            self,
            category_id: int,
            limit: int = 50,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
    ) -> List[Dict]:

        self.logger.info(
            f"Starting fetch for category_id={category_id}, "
            f"start_date={start_date}, end_date={end_date}"
        )

        results: List[Dict] = []
        # --- build date list ---
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

        # --- loop per day ---
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

                resp = requests.get(
                    BASE_URL,
                    params=params,
                    timeout=self.context.timeout,
                    headers=self.context.headers,
                )

                if resp.status_code == 429:
                    self.logger.warning("Rate limited – backing off")
                    time.sleep(10)
                    continue

                resp.raise_for_status()

                batch = resp.json()

                time.sleep(REQUEST_DELAY)  # be polite to the API

                if not batch:
                    break

                results.extend(batch)

                if len(batch) < limit:
                    break

                page += 1

        self.logger.info(f"Fetched {len(results)} discussions in date range")

        self.logger.info(f"Sample record: {results[0] if results else 'No records fetched'}")
        return results

    # def fetch(
    #     self,
    #     category_id: int,
    #     limit: int = 50,
    #     start_date: Optional[str] = None,   # rename mentally: exact date
    #     end_date: Optional[str] = None
    # ) -> List[Dict]:
    #
    #     self.logger.info(
    #         f"Starting fetch for category_id={category_id} "
    #         f"with limit={limit}, date={start_date}"
    #     )
    #
    #     results: List[Dict] = []
    #     page = 1
    #
    #     while True:
    #         params = {
    #             "CategoryID": category_id,
    #             "limit": limit,
    #             "page": page,
    #         }
    #
    #         if start_date:
    #             params["dateLastComment"] = start_date
    #
    #         resp = requests.get(
    #             BASE_URL,
    #             params=params,
    #             timeout=self.context.timeout,
    #             headers=self.context.headers,
    #         )
    #
    #         resp.raise_for_status()
    #         batch = resp.json()
    #
    #         if not batch:
    #             break
    #
    #         results.extend(batch)
    #
    #         if len(batch) < limit:
    #             break
    #
    #         page += 1
    #
    #     return results

