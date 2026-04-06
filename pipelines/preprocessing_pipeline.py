from typing import Any, Dict, List, Optional
import pandas as pd
from pipelines.base import Pipeline
from preprocessing.factory import PreprocessorFactory
from logs.logger import get_logger


class PreprocessingPipeline(Pipeline):
    """
    DataFrame-first preprocessing pipeline.

    - All preprocessors operate on the full DataFrame
    - Each preprocessor is responsible for selecting columns
    - If no columns specified, preprocessor should apply to entire DataFrame
    """

    def __init__(
        self,
        preprocessors: Optional[List[Dict[str, Any]]] = None,
        name: Optional[str] = None,
    ):
        super().__init__(name=name)

        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("Initializing PreprocessingPipeline")

        self.preprocessors = preprocessors or []

        self.logger.info(
            f"Configured preprocessors: {[p.get('name') for p in self.preprocessors]}"
        )

    @classmethod
    def from_config(cls, cfg: Dict[str, Any]):
        params = cfg.get("params", cfg)
        preprocessors = params.get("preprocessors", [])
        name = cfg.get("name") or params.get("name")

        return cls(
            preprocessors=preprocessors,
            name=name,
        )

    def execute(self, data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        if data is None or not isinstance(data, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame")

        df = data.copy()

        for i, pre_cfg in enumerate(self.preprocessors):
            name = pre_cfg.get("name")
            params = pre_cfg.get("params", {})

            self.logger.info(
                f"[Step {i+1}] Applying preprocessor: {name} with params: {params}"
            )

            try:
                pre = PreprocessorFactory.create(name, **params)
            except Exception as e:
                self.logger.exception(f"Failed to construct preprocessor '{name}': {e}")
                continue

            try:
                pre.fit(df)
                result = pre.transform(df)

                # Safety check: enforce DataFrame contract
                if not isinstance(result, pd.DataFrame):
                    raise TypeError(
                        f"Preprocessor '{name}' must return a DataFrame, got {type(result)}"
                    )

                df = result

            except Exception as e:
                self.logger.exception(f"Preprocessor '{name}' failed: {e}")

        self.logger.info("PreprocessingPipeline completed")
        return df