#fetchers/context.py
from dataclasses import dataclass
from typing import Optional

@dataclass
class FetcherContext:
    """Context for fetchers, holding common parameters."""
    api_key: Optional[str] = None
    timeout: int = 10  # Default timeout for HTTP requests in seconds
    retries: int = 3  # Default number of retries for HTTP requests
    rate_limit: Optional[int] = None  # Max requests per minute if applicable
    base_url: Optional[str] = None  # Base URL for the API endpoints
    user_agent: Optional[str] = "DataFetcher/1.0"  # Default User-Agent header
    proxy: Optional[str] = None  # Proxy URL if needed
    headers: Optional[dict] = None  # Additional headers for requests
