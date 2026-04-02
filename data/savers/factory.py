# data/savers/factory.py
from typing import Dict, Type, Any
from logs.logger import get_logger

class DataSaverFactory:
    """Factory for creating data savers by name.

    Example usage:
      saver = DataSaverFactory.get_saver('sql_server')
      saver.save(df, table_name, connector=connector, if_exists='replace', chunk_size=1000)
    """
    _registry: Dict[str, Type] = {}
    logger = get_logger("DataSaverFactory")

    @classmethod
    def register_saver(cls, name: str, saver_cls: Type) -> None:
        cls._registry[name] = saver_cls
        cls.logger.info(f"Registered data saver: {name}")

    @classmethod
    def get_saver(cls, name: str, **kwargs) -> Any:
        cls.logger.info(f"Retrieving data saver: {name} with kwargs: {kwargs}")
        saver_cls = cls._registry.get(name)
        if not saver_cls:
            cls.logger.warning(f"Data saver '{name}' not found in registry")
            return None
        return saver_cls(**kwargs)

