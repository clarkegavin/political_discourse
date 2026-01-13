# fetchers/steam_app_list_fetcher.py
from .base import Fetcher
from logs.logger import get_logger
from utils.steam_http import steam_get


class SteamAppListFetcher(Fetcher):
    """Fetch all Steam AppIDs."""
    def __init__(self, context):
        super().__init__(context)
        self.logger = get_logger(self.__class__.__name__)

    def fetch(self) -> dict:
        url = "https://api.steampowered.com/IStoreService/GetAppList/v1/"
        params = {
            "key": self.context.api_key,
            "include_games": 1,
            "cc": "ie",
            "l": "en",
            "max_results": 50000
        }

        response = steam_get(url, params, timeout=self.context.timeout)
        apps = response.json().get("response", {}).get("apps", [])

        self.logger.info(f"Retrieved {len(apps)} Steam apps")
        return {app["appid"]: app["name"] for app in apps}