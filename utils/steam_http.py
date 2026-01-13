#utils/steam_http.py
import time
import requests
from ratelimit import limits, sleep_and_retry
from logs.logger import get_logger

FIVE_MINUTES = 300
STEAM_CALLS_PER_WINDOW = 200  # official limit
logger = get_logger("utils.steam_http.steam_get")

@sleep_and_retry
@limits(calls=STEAM_CALLS_PER_WINDOW, period=FIVE_MINUTES)
def steam_get(url: str, params: dict, timeout: int = 10) -> requests.Response:
    #logger = get_logger("utils/http/steam_get")
    req = requests.Request("GET", url, params=params).prepare()
    full_url = req.url
    logger.info(f"fetching URL: {full_url}")

    try:
        response = requests.get(url, params=params, timeout=timeout)

        # Defensive handling if Steam still returns 429
        if response.status_code == 429:
            logger.info("Received 429 Too Many Requests from Steam, retrying after delay")
            retry_after = int(response.headers.get("Retry-After", 10))
            time.sleep(retry_after)
            response = requests.get(url, params=params, timeout=timeout)

        response.raise_for_status()
        return response
    except requests.exceptions.HTTPError as e:
        if response.status_code in(500, 502, 503, 504):
            logger.warning(f"Server error {response.status_code} from Steam API: {e}")
            return None
        else:
            logger.error(f"HTTP error {response.status_code} from Steam API: {e}")
            raise
    except requests.exceptions.RequestException as e:
        logger.warning(f"Request exception when accessing Steam API: {e}")
        return None

