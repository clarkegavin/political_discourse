import datetime

from .base import Fetcher
from typing import List
from logs.logger import get_logger
from utils.steam_http import steam_get
from datetime import datetime, timezone

class SteamCurrentUserFetcher(Fetcher):
    """Fetch current user data via Steam API."""
    def __init__(self, context):
        super().__init__(context)
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("Initialized SteamCurrentUserFetcher")

    def fetch(self, app_ids: List[int] ) -> list:

        url = f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1"

        results = []

        for app_id in app_ids:
            self.logger.info(f"Fetching player counts for AppID {app_id}")
            try:
                response = steam_get(
                    url,
                    params={"appid": app_id},
                    timeout=self.context.timeout
                )

                payload = response.json()
                player_count = payload.get("response", {}).get("player_count")

                self.logger.info(
                    f"Current players for AppID {app_id}: {player_count}"
                )

                results.append({
                    "AppID": app_id,
                    "current_players": player_count,
                    "current_players_fetched_at": datetime.now(timezone.utc)
                })

            except Exception as e:
                self.logger.warning(
                    f"Failed to fetch player count for AppID {app_id}: {e}"
                )
                results.append({
                    "AppID": app_id,
                    "current_players": None,
                    "current_players_fetched_at": None
                })

        return results