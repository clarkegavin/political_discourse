#data/extractor.py
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any


class DataExtractor(ABC):
    """Abstract extractor specifying read operations."""

    @abstractmethod
    def fetch_all(self) -> List[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def fetch_by_game_id(self, game_id: int) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def fetch_by_id(self, id_value: int, id_column: str = "Id") -> Optional[Dict[str, Any]]:
        raise NotImplementedError