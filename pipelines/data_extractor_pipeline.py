# pipelines/data_extractor_pipeline.py
from typing import Optional
import os
import pandas as pd

from data import ExtractorFactory
from data.extractor import DataExtractor
from logs.logger import get_logger
from pipelines.base import Pipeline


class DataExtractorPipeline(Pipeline):
    """
    ETL pipeline for a single table extractor.
    Inherits from the base Pipeline class.
    """

    def __init__(self, extractor: DataExtractor, output_csv: Optional[str] = None):
        self.extractor = extractor
        self.logger = get_logger(self.__class__.__name__)
        self.df: Optional[pd.DataFrame] = None
        self.output_csv = output_csv or os.getenv("OUTPUT_CSV", "output.csv")

    @classmethod
    def from_config(cls, cfg: dict):
        """
        Construct DataExtractorPipeline from YAML config.
        Example config keys:
          extractor_type: "roblox"
          params:
            output_csv: "output.csv"
            extractor_params:
              sample_size: 100
        """

        params = cfg.get("params", {})
        extractor_type = cfg.get("extractor_type", "roblox")
        extractor_params = params.pop("extractor_params", {})

        # Create the extractor via factory
        if extractor_type == "roblox":
            extractor = ExtractorFactory.create_roblox_extractor(**extractor_params)
        elif extractor_type == "steam":
            extractor = ExtractorFactory.create_steam_table_extractor(**extractor_params)
        else:
            raise ValueError(f"Unknown extractor type '{extractor_type}'")

        #output_csv = cfg.get("params", {}).get("output_csv")
        return cls(extractor=extractor, **params)


    def extract(self) -> None:
        """Extract data using the configured DataExtractor."""
        self.logger.info("Starting data extraction")
        self.df = pd.DataFrame(self.extractor.fetch_all())
        self.logger.info(f"Extracted {len(self.df)} rows")


    def transform(self) -> None:
        """Perform any data transformations."""
        self.logger.info("Starting transformation")

        # Example: parse datetime columns if present
        for col in ("Date", "Date_Created", "Last_Updated", "Release_Date"):
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col], errors="coerce")
        self.logger.info("Transformation complete")

    def load(self) -> None:
        """Save transformed data to CSV."""
        if self.df is not None:
            output_dir = os.path.dirname(self.output_csv)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
            self.logger.info(f"Saving data to {self.output_csv}")
            self.df.to_csv(self.output_csv, index=False)
            self.logger.info("Data saved successfully")

    def execute(self, data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Execute the full ETL process and return the resulting DataFrame.
        `data` parameter is ignored here, since extraction starts from scratch.
        """
        self.extract()
        self.transform()
        self.load()
        self.logger.info("Pipeline execution complete")
        return self.df
