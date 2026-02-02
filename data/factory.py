# data/factory.py
from typing import Optional, Type
from .sqlalchemy_connector import SQLAlchemyConnector
from .table_extractor import TableDataExtractor
from .abstract_connector import DBConnector
from .models import RobloxGame, SteamGame
from sqlalchemy.orm import DeclarativeMeta
from .steam_extractor import SteamAPIExtractor
from typing import Dict, Any
#from data.query.filter_spec import build_filters
from data.query import build_filters, build_order_by
from data.models.registry import MODEL_REGISTRY


class ExtractorFactory:
    """
    Factory for building table extractors.
    """

    @staticmethod
    def create_roblox_extractor(
        connector: Optional[DBConnector] = None,
        db_url: Optional[str] = None,
        sample_size: Optional[int] = None
    ) -> TableDataExtractor:
        """
        Returns a TableDataExtractor for RobloxGame table.
        """
        if connector is None:
            connector = SQLAlchemyConnector(db_url=db_url)
        return TableDataExtractor(connector, RobloxGame, sample_size=sample_size)

    @staticmethod
    def create_custom_extractor(
        model: Type[DeclarativeMeta],
        connector: Optional[DBConnector] = None,
        db_url: Optional[str] = None,
        sample_size: Optional[int] = None
    ) -> TableDataExtractor:
        """
        Returns a TableDataExtractor for any ORM model.
        """
        if connector is None:
            connector = SQLAlchemyConnector(db_url=db_url)
        return TableDataExtractor(connector, model, sample_size=sample_size)

    @staticmethod
    def create_steam_extractor(connector: Optional[DBConnector] = None, chunk_size: int = 100):
        if connector is None:
            connector = SQLAlchemyConnector()  # Default connection
        return SteamAPIExtractor(connector, chunk_size=chunk_size)

    @staticmethod
    def create_steam_table_extractor(
            connector: Optional[DBConnector] = None,
            db_url: Optional[str] = None,
            sample_size: Optional[int] = None
    ) -> TableDataExtractor:
        """
        Returns a TableDataExtractor for RobloxGame table.
        """
        if connector is None:
            connector = SQLAlchemyConnector(db_url=db_url)
        return TableDataExtractor(connector, SteamGame, sample_size=sample_size)

    @staticmethod
    def create_table_extractor(
            model: str,
            connector: Optional[DBConnector] = None,
            db_url: Optional[str] = None,
            sample_size: Optional[int] = None,
            order_by = None,
            filters: Optional[Dict[str, Any]] = None
    ) -> TableDataExtractor:

        if model not in MODEL_REGISTRY:
            raise ValueError(f"Unknown model '{model}'. Available: {list(MODEL_REGISTRY)}")

        model_cls = MODEL_REGISTRY[model]

        if connector is None:
            connector = SQLAlchemyConnector(db_url=db_url)

        sa_filters = build_filters(model_cls, filters) if filters else None
        sa_order_by = build_order_by(model_cls, order_by) if order_by else None

        return TableDataExtractor(
                                connector=connector,
                                model=model_cls,
                                sample_size=sample_size,
                                order_by=sa_order_by,
                                filters=sa_filters
        )
