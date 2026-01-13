#fetcher/__init__.py
from .base import Fetcher
from .factory import FetcherFactory
from .steam_app_list_fetcher import SteamAppListFetcher
from .steam_store_fetcher import SteamStoreFetcher
from .steam_review_fetcher import SteamReviewFetcher
from .steam_current_user_fetcher import SteamCurrentUserFetcher


# Register the SteamAppListFetcher with the factory
FetcherFactory.register("steam_app_list", SteamAppListFetcher)
# Register the SteamStoreFetcher with the factory
FetcherFactory.register("steam_store", SteamStoreFetcher)
# Register the SteamReviewFetcher with the factory
FetcherFactory.register("steam_review", SteamReviewFetcher)
# Register other fetchers as needed
FetcherFactory.register("steam_current_user", SteamCurrentUserFetcher)


__all__ = [
    "Fetcher",
    "FetcherFactory",
    "SteamAppListFetcher",
    "SteamStoreFetcher",
    "SteamReviewFetcher",
    "SteamCurrentUserFetcher",
]
