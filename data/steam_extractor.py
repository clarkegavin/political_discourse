# data/steam_extractor.py
from typing import List, Dict, Any, Optional
from .abstract_connector import DBConnector
from .models import SteamGame
from sqlalchemy.orm import Session
from logs.logger import get_logger
import os

class SteamAPIExtractor:
    """Fetches data from Steam API and writes to SQL Server in chunks."""

    def __init__(self, connector: DBConnector, chunk_size: int = 100):
        self._connector = connector
        self._chunk_size = chunk_size
        self.logger = get_logger(self.__class__.__name__)

    def save_chunk(self, session: Session, games: List[Dict[str, Any]]):
        objs = [SteamGame(**g) for g in games]
        session.bulk_save_objects(objs)
        session.commit()
        self.logger.info(f"Saved {len(games)} rows to Steam table")

    def save_data(self, games: List[Dict[str, Any]]):
        """Save API-fetched data in chunks"""
        with self._connector.get_session() as session:
            for i in range(0, len(games), self._chunk_size):
                chunk = games[i : i + self._chunk_size]
                self.save_chunk(session, chunk)
