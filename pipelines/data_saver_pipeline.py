# pipelines/data_saver_pipeline.py
from typing import Optional, Dict, Any
from pipelines.base import Pipeline
from logs.logger import get_logger
from data.savers import DataSaverFactory
from data.sqlalchemy_connector import SQLAlchemyConnector
import pandas as pd

class DataSaverPipeline(Pipeline):
    """Pipeline to persist a pandas DataFrame to a database using DataSaverFactory.

    Configuration (params):
      saver_name: name registered in DataSaverFactory (default: "sql_server")
      table_name: destination table name (required)
      if_exists: 'replace'|'append'|'fail' (default: 'replace')
      chunk_size: rows per chunk (default: 1000)
      schema: optional DB schema
      connector_params: dict passed to SQLAlchemyConnector constructor (optional)

    Usage:
      pipeline = DataSaverPipeline.from_config(cfg)
      pipeline.execute(df)
    """

    def __init__(
        self,
        saver_name: str = "sql_server",
        table_name: Optional[str] = None,
        if_exists: str = "replace",
        chunk_size: int = 1000,
        schema: Optional[str] = None,
        connector_params: Optional[Dict[str, Any]] = None,
        name: Optional[str] = None,
    ):
        super().__init__(name=name or "DataSaverPipeline")
        self.logger = get_logger(self.__class__.__name__)
        self.saver_name = saver_name
        self.table_name = table_name
        self.if_exists = if_exists
        self.chunk_size = chunk_size
        self.schema = schema
        self.connector_params = connector_params or {}

    @classmethod
    def from_config(cls, cfg: Dict[str, Any]):
        params = cfg.get("params", {})
        return cls(
            saver_name=params.get("saver_name", "sql_server"),
            table_name=params.get("table_name"),
            if_exists=params.get("if_exists", "replace"),
            chunk_size=params.get("chunk_size", 1000),
            schema=params.get("schema"),
            connector_params=params.get("connector_params", {}),
            name=cfg.get("name"),
        )

    def execute(self, data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Persist the provided DataFrame to the configured table.

        Parameters
        ----------
        data : pd.DataFrame
            The DataFrame to save.

        Returns
        -------
        pd.DataFrame
            The same DataFrame (unchanged) for chaining.
        """

        self.logger.info("Starting DataSaverPipeline execution")
        if data is None:
            raise ValueError("DataSaverPipeline requires a pandas DataFrame passed as `data` to execute()")
        if not isinstance(data, pd.DataFrame):
            raise TypeError("DataSaverPipeline.execute expects a pandas DataFrame")
        if not self.table_name:
            raise ValueError("DataSaverPipeline requires `table_name` to be configured")

        self.logger.info(f"DataSaverPipeline: saving DataFrame to {self.table_name} using saver '{self.saver_name}'")

        # Instantiate saver
        saver = DataSaverFactory.get_saver(self.saver_name)
        if saver is None:
            raise ValueError(f"No saver registered with name '{self.saver_name}'")

        # Create connector using existing SQLAlchemyConnector
        connector = SQLAlchemyConnector(**self.connector_params) if self.connector_params is not None else SQLAlchemyConnector()

        # Perform save
        saver.save(
            df=data,
            table_name=self.table_name,
            connector=connector,
            if_exists=self.if_exists,
            chunk_size=self.chunk_size,
            schema=self.schema,
        )

        self.logger.info("Data saved successfully by DataSaverPipeline")
        return data

