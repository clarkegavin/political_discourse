# fetchers/oireachtas_question_fetcher.py
import requests
from typing import Dict, List
from logs.logger import get_logger
from .base import Fetcher
import time



class OireachtasQuestionFetcher(Fetcher):
    BASE_URL = "https://api.oireachtas.ie/v1/questions"
    MAX_SKIP = 20000
    MAX_RETRIES = 5
    RETRY_SLEEP = 2  # seconds

    def __init__(self, context):
        super().__init__(context)
        self.last_expected_count = None
        self.logger = get_logger(self.__class__.__name__)

    def fetch(
        self,
        date_start: str,
        date_end: str,
        qtypes: str = "oral,written",
        limit: int = 1000,
        probe_only: bool = False,
    ) -> List[Dict]:

        # self.logger.info(
        #     f"Fetching questions from {date_start} to {date_end} "
        #     f"of types {qtypes} with limit {limit}"
        # )

        all_results = []
        skip = 0
        total_expected = None



        while True:
            params = {
                "date_start": date_start,
                "date_end": date_end,
                "qtype": qtypes,
                "limit": limit,
                "skip": skip,
            }

            # response = requests.get(
            #     self.BASE_URL,
            #     params=params,
            #     timeout=self.context.timeout,
            #     headers=self.context.headers,
            # )

            for attempt in range(1, self.MAX_RETRIES + 1):
                try:
                    response = requests.get(
                        self.BASE_URL,
                        params=params,
                        timeout=self.context.timeout,
                        headers=self.context.headers,
                    )
                    # self.logger.info(
                    #     f"Fetched {len(response.content)} bytes "
                    #     f"for skip={skip} (attempt {attempt})"
                    # )
                    break
                except requests.exceptions.RequestException as e:
                    self.logger.warning(
                        f"Request failed (attempt {attempt}/{self.MAX_RETRIES}): {e}"
                    )
                    if attempt == self.MAX_RETRIES:
                        raise
                    self.logger.info(f"Retrying in {self.RETRY_SLEEP} seconds...")
                    time.sleep(self.RETRY_SLEEP)
                    self.logger.info("Retrying now...")

            if response.status_code == 500:
                self.logger.warning(f"500 error at skip={skip}, stopping")
                break

            response.raise_for_status()
            data = response.json()

            if total_expected is None:
                total_expected = (
                    data.get("head", {})
                        .get("counts", {})
                        .get("resultCount")
                )
                self.logger.info(f"Volume for {date_start} to {date_end}: {total_expected}")

                if probe_only:
                    #self.logger.info("Probe only mode: fetching single page to estimate total count")
                    break

            self.last_expected_count = total_expected

            results = data.get("results", [])
            if not results:
                break

            all_results.extend(results)
            skip += limit

            if total_expected and len(all_results) >= total_expected:
                self.logger.info("Fetched all expected results")
                break

            if skip > self.MAX_SKIP:
                self.logger.error("Skip cap exceeded")
                break

        self.logger.info(f"Fetched total {len(all_results)} records")
        return all_results
