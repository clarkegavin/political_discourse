# utils/request_rate_limiter.py
from ratelimit import limits, sleep_and_retry
import requests
import time

ONE_MINUTE = 60
MAX_REQUESTS_PER_MINUTE = 100  # adjust based on Steam API tolerance

@sleep_and_retry
@limits(calls=MAX_REQUESTS_PER_MINUTE, period=ONE_MINUTE)
def rate_limited_request(url, params=None, timeout=10):
    """Send a GET request respecting rate limits and retry on 429."""
    response = requests.get(url, params=params, timeout=timeout)

    retries = 0
    while response.status_code == 429 and retries < 5:
        retry_after = int(response.headers.get("Retry-After", 5))
        time.sleep(retry_after)
        response = requests.get(url, params=params, timeout=timeout)
        retries += 1

    response.raise_for_status()
    return response
