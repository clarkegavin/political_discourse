import time
from typing import List, Dict, Optional, Iterable
import requests
from logs.logger import get_logger
from .base import Fetcher

REQUEST_DELAY = 1.0
BASE_URL = "https://www.boards.ie/api/v2/comments"


class BoardsCommentsFetcher(Fetcher):
    """Fetcher for boards.ie comments API."""

    def __init__(self, context):
        super().__init__(context)
        self.logger = get_logger(self.__class__.__name__)

    def fetch_batches(
        self,
        discussion_id: int,
        limit: int = 500,
        date_start: Optional[str] = None,
        date_end: Optional[str] = None,
    ) -> Iterable[List[Dict]]:
        """Generator yielding batches of comments for a discussion.

        date_start/date_end can be ISO date strings; if both provided will be passed
        as dateInserted=[start,end] to the API.
        """
        page = 1
        self.logger.info(f"Fetching comments for discussion={discussion_id} start={date_start} end={date_end}")

        while True:
            params = {
                "discussionid": discussion_id,
                "limit": limit,
                "page": page,
            }

            if date_start and date_end:
                params["dateInserted"] = f"[{date_start},{date_end}]"
            elif date_start:
                params["dateInserted"] = f"[{date_start},{date_start}]"

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
            time.sleep(REQUEST_DELAY)

            if not batch:
                break

            yield batch

            if len(batch) < limit:
                break
            page += 1

    def fetch(self, discussion_id: int, limit: int = 500, date_start: Optional[str] = None, date_end: Optional[str] = None) -> List[Dict]:
        all_results = []
        for batch in self.fetch_batches(discussion_id, limit, date_start, date_end):
            all_results.extend(batch)
        return all_results
