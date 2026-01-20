# fetchers/oireachtas_question_fetcher.py
import requests
from typing import Dict, List
from logs.logger import get_logger
from .base import Fetcher

class OireachtasQuestionFetcher(Fetcher):
    BASE_URL = "https://api.oireachtas.ie/v1/questions"
    MAX_SKIP = 20000

    def __init__(self, context):
        super().__init__(context)
        self.logger = get_logger(self.__class__.__name__)

    def fetch(
        self,
        date_start: str,
        date_end: str,
        qtypes: str = "oral,written",
        limit: int = 50,
    ) -> List[Dict]:

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

            response = requests.get(
                self.BASE_URL,
                params=params,
                timeout=self.context.timeout,
                headers=self.context.headers,
            )

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
                self.logger.info(f"Expected {total_expected} results")

            results = data.get("results", [])
            if not results:
                break

            all_results.extend(results)
            skip += limit

            if total_expected and len(all_results) >= total_expected:
                break

            if skip > self.MAX_SKIP:
                self.logger.error("Skip cap exceeded")
                break

        return all_results
