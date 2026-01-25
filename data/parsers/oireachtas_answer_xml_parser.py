import requests
from datetime import datetime
from typing import Optional, Dict
from lxml import etree
from logs.logger import get_logger
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

AKN_NS = {"akn": "http://docs.oasis-open.org/legaldocml/ns/akn/3.0/CSD13"}


class OireachtasAnswerXMLParser:

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Connection": "close",  # important for this API
        })
        retries = Retry(
            total=3,
            backoff_factor=1.5,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"],
        )

        adapter = HTTPAdapter(
            max_retries=retries,
            pool_connections=5,
            pool_maxsize=5,
        )

        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)


    def fetch_xml(self, uri: str) -> Optional[str]:
        #self.logger.info(f"Fetching XML from {uri}")

        # resp = requests.get(
        #     uri,
        #     headers={"User-Agent": "Mozilla/5.0"},
        #     timeout=30,
        # )

        try:
            resp = self.session.get(uri, timeout=(5, 60))
        except requests.exceptions.RequestException as e:
            self.logger.warning(f"XML fetch failed for {uri}: {e}")
            return None

        if resp.status_code != 200:
            self.logger.warning(f"Failed to fetch XML from {uri}, status code: {resp.status_code}")
            return None
        return resp.text

    def parse(self, xml_text: str) -> Dict:
        #self.logger.info("Parsing answer XML")
        root = etree.fromstring(xml_text.encode("utf-8"))

        speech = root.find(".//akn:speech", namespaces=AKN_NS)
        if speech is None:
            return {}

        # Speaker
        speaker_ref = speech.get("by")
        speaker_name = speaker_ref.lstrip("#") if speaker_ref else None

        # Recorded time
        time_el = speech.find(".//akn:recordedTime", namespaces=AKN_NS)
        recorded_time = None
        if time_el is not None:
            recorded_time = datetime.fromisoformat(
                time_el.get("time").replace("Z", "+00:00")
            )

        # Speech text
        paragraphs = speech.findall(".//akn:p", namespaces=AKN_NS)
        text = "\n".join("".join(p.itertext()).strip() for p in paragraphs)

        #self.logger.info(f"Extracted speech by {speaker_name} at {recorded_time}")
        return {
            "speaker": speaker_name,
            "recorded_time": recorded_time,
            "text": text,
        }
