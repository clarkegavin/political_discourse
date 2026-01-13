# pipelines/filter_pipeline.py
from filters.factory import FilterFactory
import pandas as pd
from logs.logger import get_logger
from pipelines.base import Pipeline



class FilterPipeline(Pipeline):
    logger = get_logger("FilterPipeline")

    def __init__(self, filter_configs: list[dict]):
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("Initializing FilterPipeline with filters:")
        self.filters = [
            FilterFactory.create_filter(config["name"], **config.get("params", {}))
            for config in filter_configs
        ]
        self.logger.info(f"Initialized {len(self.filters)} filters.")

    def fit(self, data: pd.DataFrame):
        """ fit all filters in the pipeline """
        self.logger.info("Starting fit in FilterPipeline")
        for filter_instance in self.filters:
            self.logger.info(f"Fitting filter: {filter_instance.__class__.__name__}")
            filter_instance.fit(data)
            #data = filter_instance.transform(data)
        return self

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """ apply all filters in the pipeline """
        self.logger.info("Starting transform in FilterPipeline")
        for filter_instance in self.filters:
            data = filter_instance.transform(data)
        return data

    def fit_transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """ fit and apply all filters in the pipeline """
        self.logger.info("Starting fit_transform in FilterPipeline")
        self.fit(data)
        return self.transform(data)

    @classmethod
    def from_config(cls, cfg: dict) -> "FilterPipeline":
        cls.logger.info(f"FilterPipeline cfg keys: {list(cfg.keys())}")
        # params = cfg.get("params", cfg)
        # filter_configs = params.get("filter_features", [])

        filter_configs = cfg.get("filter_features", [])
        return cls(filter_configs)

    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
        """ Execute the full pipeline: fit and transform """
        self.logger.info("Executing FilterPipeline")
        data = self.fit_transform(data)
        self.logger.info(f"FilterPipeline execution completed - data shape is {data.shape}")
        return data