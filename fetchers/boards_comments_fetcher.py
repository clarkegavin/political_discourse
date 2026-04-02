import time
from typing import List, Dict, Optional, Iterable
import requests
import random
from requests.exceptions import RequestException
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
        max_retries: int = 5,
        backoff_factor: float = 1.0,
    ) -> Iterable[List[Dict]]:
        """Generator yielding batches of comments for a discussion.

        date_start/date_end can be ISO date strings; if both provided will be passed
        as dateInserted=[start,end] to the API.

        This method will retry transient network/server errors and rate-limits
        using exponential backoff with jitter. Parameters:
          - max_retries: how many times to retry a single request before failing
          - backoff_factor: base seconds used for exponential backoff calculation
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

            # Per-request retry loop
            attempt = 0
            while True:
                attempt += 1
                try:
                    resp = requests.get(
                        BASE_URL,
                        params=params,
                        timeout=self.context.timeout,
                        headers=self.context.headers,
                    )
                except RequestException as exc:
                    if attempt > max_retries:
                        self.logger.error(
                            f"Request failed after {max_retries} attempts: {exc}",
                            exc_info=True,
                        )
                        raise
                    sleep_time = backoff_factor * (2 ** (attempt - 1)) + random.uniform(0, 1)
                    self.logger.warning(
                        f"Request error (attempt {attempt}/{max_retries}): {exc}. Retrying in {sleep_time:.1f}s"
                    )
                    time.sleep(sleep_time)
                    continue

                # Handle rate limiting specially (Retry-After header respected if present)
                if resp.status_code == 429:
                    if attempt > max_retries:
                        self.logger.error("Rate limited and max retries exceeded")
                        resp.raise_for_status()

                    retry_after = resp.headers.get("Retry-After")
                    if retry_after is not None:
                        try:
                            wait = int(retry_after)
                        except ValueError:
                            wait = backoff_factor * (2 ** (attempt - 1)) + random.uniform(0, 1)
                    else:
                        wait = backoff_factor * (2 ** (attempt - 1)) + random.uniform(0, 1)

                    self.logger.warning(
                        f"Rate limited – backing off for {wait}s (attempt {attempt}/{max_retries})"
                    )
                    time.sleep(wait)
                    continue

                # Server errors (5xx) - retryable
                if 500 <= resp.status_code < 600:
                    if attempt > max_retries:
                        resp.raise_for_status()
                    sleep_time = backoff_factor * (2 ** (attempt - 1)) + random.uniform(0, 1)
                    self.logger.warning(
                        f"Server error {resp.status_code} (attempt {attempt}/{max_retries}), retrying in {sleep_time:.1f}s"
                    )
                    time.sleep(sleep_time)
                    continue

                # Other statuses - either success or fatal client error
                resp.raise_for_status()

                batch = resp.json()
                # leave a small delay to avoid hitting the API too fast
                time.sleep(REQUEST_DELAY)

                if not batch:
                    return

                yield batch

                if len(batch) < limit:
                    return

                page += 1

    def fetch(self, discussion_id: int, limit: int = 500, date_start: Optional[str] = None, date_end: Optional[str] = None, max_retries: int = 5, backoff_factor: float = 1.0) -> List[Dict]:
        all_results = []
        for batch in self.fetch_batches(discussion_id, limit, date_start, date_end, max_retries, backoff_factor):
            all_results.extend(batch)
        return all_results
