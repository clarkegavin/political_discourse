# fetchers/oireachtas_party_fetcher.py
import requests
import time
from typing import List, Dict, Generator
from logs.logger import get_logger
from .base import Fetcher


class OireachtasPartyFetcher(Fetcher):
    BASE_URL = "https://api.oireachtas.ie/v1/parties"
    MAX_SKIP = 200000
    MAX_RETRIES = 5
    RETRY_SLEEP = 2  # seconds

    def __init__(self, context):
        super().__init__(context)
        self.logger = get_logger(self.__class__.__name__)
        self.last_expected_count = None

    def fetch_batches(
        self,
        chamber: str = "dail",
        house_no: int = 34,
        limit: int = 50,
        request_delay: float = 1.0,
        probe_only: bool = False,
    ) -> Generator[List[Dict], None, None]:
        """Generator that yields batches of party results from the API.

        Params are configurable. Continues paging using skip/limit until
        the API-reported resultCount is exhausted. Sleeps request_delay
        seconds between requests.
        """
        skip = 0
        total_expected = None

        while True:
            params = {
                "chamber": chamber,
                "house_no": house_no,
                "limit": limit,
                "skip": skip,
            }

            # perform request with retry logic
            resp = None
            for attempt in range(1, self.MAX_RETRIES + 1):
                try:
                    resp = requests.get(
                        self.BASE_URL,
                        params=params,
                        timeout=self.context.timeout,
                        headers=self.context.headers,
                    )
                    break
                except requests.exceptions.RequestException as e:
                    self.logger.warning(
                        f"Request failed (attempt {attempt}/{self.MAX_RETRIES}): {e}"
                    )
                    if attempt == self.MAX_RETRIES:
                        raise
                    time.sleep(self.RETRY_SLEEP)

            if resp is None:
                # Shouldn't happen, but guard for static analysis and safety
                self.logger.error("No response obtained from requests")
                break

            if resp.status_code == 500:
                self.logger.warning(f"500 error at skip={skip}, stopping parties fetch")
                break

            resp.raise_for_status()
            data = resp.json()

            if total_expected is None:
                total_expected = (
                    data.get("head", {}).get("counts", {}).get("resultCount")
                )
                self.last_expected_count = total_expected
                self.logger.info(f"Parties volume reported: {total_expected}")
                if probe_only:
                    break

            batch = data.get("results", [])
            if not batch:
                self.logger.info(f"Empty page at skip={skip}; stopping")
                break

            yield batch

            skip += limit
            time.sleep(request_delay)

            if total_expected is not None and skip >= total_expected:
                self.logger.info("Reached expected resultCount; stopping pagination")
                break

            if skip > self.MAX_SKIP:
                self.logger.error("Skip cap exceeded")
                break

    def fetch(
        self,
        chamber: str = "dail",
        house_no: int = 34,
        limit: int = 50,
        request_delay: float = 1.0,
        probe_only: bool = False,
    ) -> List[Dict]:
        """Convenience method that returns all results as a list by
        consuming the batch generator. Useful for callers that expect a list.
        """
        all_results: List[Dict] = []
        for batch in self.fetch_batches(chamber, house_no, limit, request_delay, probe_only):
            all_results.extend(batch)
        return all_results
