#data/services/oireachtas_question_service.py
from datetime import date, timedelta
from calendar import monthrange
from concurrent.futures import ThreadPoolExecutor
import threading

from data.parsers.oireachtas_answer_xml_parser import OireachtasAnswerXMLParser

SAFE_THRESHOLD = 9_500

class OireachtasQuestionIngestionService:

    def __init__(self, fetcher, extractor=None, chunk_size=100):
        self.fetcher = fetcher
        self.logger = fetcher.logger
        self.extractor = extractor
        self.chunk_size = chunk_size
        self.answer_parser = OireachtasAnswerXMLParser()
        # caching
        self._answer_cache = {}  # cache for fetched answer XMLs
        self._cache_lock = threading.Lock()  # ensures only one thread writes to cache at a time



    def ingest(self, start: date, end: date):
        for month_start, month_end in self._month_ranges(start, end):
            self.logger.info(f"Checking volume for {month_start} → {month_end}")
            self._answer_cache.clear()  # clear cache for each month

            # lightweight probe
            self.fetcher.fetch(
                date_start=month_start.isoformat(),
                date_end=month_end.isoformat(),
                limit=1,
                probe_only=True,
            )

            expected = self.fetcher.last_expected_count or 0
            self.logger.info(f"Volume for {month_start} → {month_end}: {expected}")

            if expected < SAFE_THRESHOLD:
                self.logger.info(f"Fetching month {month_start} → {month_end}")
                results = self.fetcher.fetch(
                    date_start=month_start.isoformat(),
                    date_end=month_end.isoformat(),
                )

                self._enrich_records(results)
                self._save_in_chunks(results)
            else:
                self.logger.warning(f"High volume ({expected}); falling back to daily fetch")
                for day in self._day_ranges(month_start, month_end):
                    self.logger.info(f"Fetching day {day}")
                    results = self.fetcher.fetch(
                        date_start=day.isoformat(),
                        date_end=day.isoformat(),
                    )
                    self._enrich_records(results)
                    self._save_in_chunks(results)

    def _save_in_chunks(self, records: list[dict]):
        self.logger.info(f"Saving total {len(records)} records in chunks of {self.chunk_size}")
        if not self.extractor:
            return
        for i in range(0, len(records), self.chunk_size):
            chunk = records[i:i + self.chunk_size]
            self.logger.info(f"Saving chunk of {len(chunk)} records")
            self.extractor.save_data(chunk)
            self.logger.info("Chunk saved")

    @staticmethod
    def _month_ranges(start, end):
        current = date(start.year, start.month, 1)
        while current <= end:
            last_day = monthrange(current.year, current.month)[1]
            month_end = date(current.year, current.month, last_day)
            yield current, min(month_end, end)
            current = month_end + timedelta(days=1)

    @staticmethod
    def _day_ranges(start, end):
        current = start
        while current <= end:
            yield current
            current += timedelta(days=1)


    def _enrich_records(self, records, max_workers=8):
        self.logger.info(f"Enriching {len(records)} records with answers using {max_workers} workers")

        def enrich(r):
            q = r.get("question", r)
            debate = q.get("debateSection", {})
            xml_uri = debate.get("formats", {}).get("xml", {}).get("uri")

            if not xml_uri:
                return

            # Check cache first
                # check cache without lock first
            cached = self._answer_cache.get(xml_uri)
            if cached:
                parsed, xml = cached
            else:
                # fetch & parse outside the lock
                xml = self.answer_parser.fetch_xml(xml_uri)
                if not xml:
                    return
                parsed = self.answer_parser.parse(xml)

                # write to cache with lock
                with self._cache_lock:
                    # double-check if another thread already wrote it
                    if xml_uri not in self._answer_cache:
                        self._answer_cache[xml_uri] = (parsed, xml)

            # stash enrichment onto the record
            r["_answer_xml"] = xml
            r["_answer_text"] = parsed.get("text")
            r["_answer_speaker"] = parsed.get("speaker")
            r["_answer_recorded_time"] = parsed.get("recorded_time")
            # xml = self.answer_parser.fetch_xml(xml_uri)
            # if not xml:
            #     return
            #
            # parsed = self.answer_parser.parse(xml)
            #
            # # stash enrichment onto the record
            # r["_answer_xml"] = xml
            # r["_answer_text"] = parsed.get("text")
            # r["_answer_speaker"] = parsed.get("speaker")
            # recorded_time = parsed.get("recorded_time")
            # # JSON-safe: store as ISO string
            # # Store a JSON-safe string separately
            # # r["_answer_recorded_time_json"] = (
            # #     recorded_time.isoformat() if recorded_time else None
            # # )
            #
            # # Store Python datetime separately for SQL
            # r["_answer_recorded_time"] = recorded_time



        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            list(executor.map(enrich, records))
