# fetchers/oireachtas_members_fetcher.py
import requests
import time
from typing import List, Dict, Generator
from logs.logger import get_logger
from .base import Fetcher


class OireachtasMembersFetcher(Fetcher):
    BASE_URL = "https://api.oireachtas.ie/v1/members"
    MAX_SKIP = 500000
    MAX_RETRIES = 5
    RETRY_SLEEP = 2  # seconds

    def __init__(self, context):
        super().__init__(context)
        self.logger = get_logger(self.__class__.__name__)
        self.last_expected_count = None

    def fetch_batches(
        self,
        date_start: str,
        date_end: str,
        chamber: str = "dail",
        house_no: int = 34,
        limit: int = 50,
        request_delay: float = 1.0,
        probe_only: bool = False,
    ) -> Generator[List[Dict], None, None]:
        """Yield batches of member records using skip/limit pagination."""
        skip = 0
        total_expected = None

        while True:
            params = {
                "date_start": date_start,
                "date_end": date_end,
                "chamber": chamber,
                "house_no": house_no,
                "limit": limit,
                "skip": skip,
            }

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
                self.logger.error("No response from members API")
                break

            if resp.status_code == 500:
                self.logger.warning(f"500 error at skip={skip}, stopping members fetch")
                break

            resp.raise_for_status()
            data = resp.json()

            if total_expected is None:
                total_expected = (
                    data.get("head", {}).get("counts", {}).get("resultCount")
                )
                self.last_expected_count = total_expected
                self.logger.info(f"Members volume reported: {total_expected}")
                if probe_only:
                    break

            batch = data.get("results", [])
            if not batch:
                self.logger.info(f"Empty results page at skip={skip}; stopping")
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
        date_start: str,
        date_end: str,
        chamber: str = "dail",
        house_no: int = 34,
        limit: int = 50,
        request_delay: float = 1.0,
        probe_only: bool = False,
    ) -> List[Dict]:
        all_results: List[Dict] = []
        for batch in self.fetch_batches(date_start, date_end, chamber, house_no, limit, request_delay, probe_only):
            all_results.extend(batch)
        return all_results

