# pipelines/imputer_pipeline.py
from typing import Any, Dict, List, Optional
import pandas as pd
from pipelines.base import Pipeline
from imputers.factory import ImputerFactory
from logs.logger import get_logger


class ImputerPipeline(Pipeline):
    """Pipeline that applies one or more imputers to a DataFrame.

    Config (params):
      imputers:
        - name: simple
          params:
            columns: ['Description', 'Current_Players']
            numeric_strategy: 'mean'

    Behavior:
      - Builds imputers via ImputerFactory and applies them sequentially to the DataFrame.
    """

    def __init__(self, imputers: Optional[List[Dict[str, Any]]] = None, name: Optional[str] = None):
        super().__init__(name=name)
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("Initializing ImputerPipeline")
        self.raw_imputers = imputers or []
        self.imputers = self._build_imputers(self.raw_imputers)
        self.logger.info(f"Configured imputers: {[i.get('name') for i in self.raw_imputers]}")

    @classmethod
    def from_config(cls, cfg: Dict[str, Any]):
        params = cfg.get('params', cfg)
        imputers = params.get('imputers', [])
        name = cfg.get('name') or params.get('name')
        return cls(imputers, name=name)

    def _build_imputers(self, imputers_cfg: List[Dict[str, Any]]):
        built = []
        for icfg in imputers_cfg:
            name = icfg.get('name')
            params = icfg.get('params', {})
            self.logger.info(f"Creating imputer '{name}' with params: {params}")
            try:
                im = ImputerFactory.create(name, **params)
                built.append(im)
            except Exception as e:
                self.logger.error(f"Failed to create imputer '{name}': {e}")
        return built

    def execute(self, data: Optional[pd.DataFrame] = None) -> Any:
        if data is None or not isinstance(data, pd.DataFrame):
            raise ValueError("ImputerPipeline expects a pandas DataFrame as input")

        df = data.copy()
        for im in self.imputers:
            try:
                im.fit(df)
                df = im.transform(df)
            except Exception as e:
                self.logger.exception(f"Imputer {im.__class__.__name__} failed: {e}")
        return df

