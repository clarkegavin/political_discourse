# data/table_extractor.py
from typing import List, Optional, Dict, Any, Type
from sqlalchemy import select, func
from .extractor import DataExtractor
from .abstract_connector import DBConnector
from sqlalchemy.orm import DeclarativeMeta
from logs.logger import get_logger


class TableDataExtractor(DataExtractor):
    """
    Generic table extractor that works with any SQLAlchemy ORM model.

    Supports full dataset or a sample (limit).
    """

    def __init__(self, connector: DBConnector, model: Type[DeclarativeMeta], sample_size: Optional[int] = None):
        self._connector = connector
        self._model = model
        self._sample_size = sample_size
        self.logger = get_logger(self.__class__.__name__)

    def fetch_all(self) -> List[Dict[str, Any]]:
        self.logger.info(f"Fetching all rows from {self._model.__tablename__}")
        with self._connector.get_session() as session:
            stmt = select(self._model)
            # SQL Server random ordering
            stmt = stmt.order_by(func.newid())

            if self._sample_size:
                stmt = stmt.limit(self._sample_size)
                self.logger.info(f"Randomly sampling {self._sample_size} rows for sample")
            rows = session.execute(stmt).scalars().all()
            return [r.to_dict() for r in rows]

    def fetch_by_game_id(self, game_id: int) -> Optional[Dict[str, Any]]:
        with self._connector.get_session() as session:
            stmt = select(self._model).filter(getattr(self._model, "gameId") == game_id)
            result = session.execute(stmt).scalars().first()
            return result.to_dict() if result else None

    def fetch_by_id(self, id_value: int, id_column: str = "Id") -> Optional[Dict[str, Any]]:
        with self._connector.get_session() as session:
            stmt = select(self._model).filter(getattr(self._model, id_column) == id_value)
            result = session.execute(stmt).scalars().first()
            return result.to_dict() if result else None
