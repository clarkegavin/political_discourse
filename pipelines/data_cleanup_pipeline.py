# pipelines/data_cleanup_pipeline.py
from typing import Any, Dict, List, Optional
import pandas as pd
from pipelines.base import Pipeline
from preprocessing.factory import PreprocessorFactory
from logs.logger import get_logger


class DataCleanupPipeline(Pipeline):
    """Pipeline to run data cleanup preprocessors sequentially.

    Example config in YAML:
    cleanup_steps:
      - name: remove_duplicates
        params:
          field: "Genre"
          keep: "first"

    The pipeline creates each preprocessor via PreprocessorFactory and calls
    fit/transform similarly to PreprocessingPipeline but operates on the full
    DataFrame so cleanup steps can modify rows/columns.
    """

    def __init__(self, cleanup_steps: List[Dict[str, Any]]):
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("Initializing DataCleanupPipeline")
        self.cleanup_steps = cleanup_steps or []
        # build preprocessor instances
        # self.steps = [
        #     PreprocessorFactory.create(step["name"], **step.get("params", {}))
        #     for step in self.cleanup_steps
        # ]
        self.steps = []

        for step in self.cleanup_steps:
            name = step["name"]
            params = step.get("params") or {}

            self.logger.info(f"Creating cleanup step '{name}' with params: {params}")

            self.steps.append(
                PreprocessorFactory.create(name, **params)
            )

        self.logger.info(f"Initialized {len(self.steps)} cleanup steps")
        self.logger.info(f"Raw cleanup steps config: {self.cleanup_steps}")

    @classmethod
    def from_config(cls, cfg: Dict[str, Any]) -> "DataCleanupPipeline":
        #steps = cfg.get("cleanup_steps", [])
        # Handle both direct and nested "params"
        params = cfg.get("params", cfg)
        steps = params.get("cleanup_steps", [])
        return cls(steps)

    def execute(self, data: Optional[pd.DataFrame] = None) -> Optional[pd.DataFrame]:
        self.logger.info("Executing DataCleanupPipeline")
        if data is None:
            self.logger.warning("No data provided to DataCleanupPipeline.execute; returning None")
            return None
        df = data
        for step in self.steps:
            try:
                self.logger.info(f"Applying cleanup step: {step.__class__.__name__}")
                step.fit(df)
                df = step.transform(df)

                # --------------------------
                # Special check for StopwordRemover on 'Description' field

                # if step.__class__.__name__ == "StopwordRemover":
                #     still_present = df['Description'].str.contains('roblox', case=False, na=False).sum()
                #     self.logger.info(f"'roblox' still present in {still_present} rows after StopwordRemover")
                #     # Or randomly sample 10 rows
                #     # Filter rows where roblox still appears (case-insensitive)
                #     roblox_rows = df[df['Description'].str.contains('roblox', case=False, na=False)]
                #
                #     # Sample from THAT subset only
                #     if not roblox_rows.empty:
                #         sample_texts = roblox_rows['Description'].sample(
                #             min(5, len(roblox_rows)),
                #             random_state=42
                #         ).tolist()
                #         for i, t in enumerate(sample_texts, 1):
                #             self.logger.info(f"{i}: {t}")
                #     else:
                #         self.logger.info("No rows contain 'roblox' after StopwordRemover.")

                # ensure df stays a DataFrame where reasonable
                if hasattr(df, "reset_index"):
                    try:
                        df = df.reset_index(drop=True)
                    except Exception:
                        pass
            except Exception as e:
                self.logger.exception(f"Cleanup step {step.__class__.__name__} failed: {e}")
        self.logger.info(f"DataCleanupPipeline completed - data shape: {getattr(df, 'shape', 'unknown')}")
        return df

