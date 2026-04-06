# pipelines/merge_pipeline.py
from typing import Any, Dict, Iterable, List, Optional, Union
import pandas as pd

from data.extractor import DataExtractor
from logs.logger import get_logger
from pipelines.base import Pipeline
from data.factory import ExtractorFactory

TransformSpec = Dict[str, Any]

class MergeTablesPipeline(Pipeline):
    """Simple pipeline to merge two tables/DataFrames."""

    def __init__(
        self,
        left_extractor_or_df: Union[DataExtractor, pd.DataFrame],
        right_extractor_or_df: Union[DataExtractor, pd.DataFrame],
        join_key: Optional[Union[str, Iterable[str]]] = None,
        left_on: Optional[Union[str, Iterable[str]]] = None,
        right_on: Optional[Union[str, Iterable[str]]] = None,
        left_fields: Optional[Union[Dict[str, str], Iterable[str]]] = None,
        right_fields: Optional[Union[Dict[str, str], Iterable[str]]] = None,
        how: str = "inner",
        suffixes: tuple = ("_left", "_right"),
        transformations: Optional[Dict[str, TransformSpec]] = None,
        name: Optional[str] = None,
    ):
        super().__init__(name=name)
        self.logger = get_logger(self.__class__.__name__)

        self.left = left_extractor_or_df
        self.right = right_extractor_or_df
        self.left_fields = left_fields
        self.right_fields = right_fields
        self.how = how
        self.suffixes = suffixes
        self.transformations = transformations or {}

        # Normalize join keys
        if join_key and (left_on or right_on):
            raise ValueError("Provide either join_key OR left_on/right_on, not both")

        if left_on or right_on:
            self.logger.info(f"Configuring merge with left_on: {left_on} and right_on: {right_on}")
            self.left_on = list(left_on) if isinstance(left_on, (list, tuple)) else [left_on]
            self.right_on = list(right_on) if isinstance(right_on, (list, tuple)) else [right_on]
            self.join_key = None
        elif join_key:
            self.join_key = list(join_key) if isinstance(join_key, (list, tuple)) else [join_key]
            self.left_on = self.right_on = None
        else:
            raise ValueError("Must provide either join_key or left_on/right_on")

    # --- Helpers ---
    def _extract(self, source: Union[DataExtractor, pd.DataFrame], side: str) -> pd.DataFrame:
        self.logger.info(f"Extracting {side} source for MergeTablesPipeline")
        if isinstance(source, pd.DataFrame):
            return source.copy()
        if hasattr(source, "fetch_all") and callable(getattr(source, "fetch_all")):
            return pd.DataFrame(source.fetch_all())
        raise TypeError(f"{side} source must be a DataFrame or DataExtractor")

    def _select_fields(self, df: pd.DataFrame, fields: Optional[Union[Dict[str, str], Iterable[str]]]):
        self.logger.info(f"Selecting fields for DataFrame with shape {df.shape}")
        if not fields:
            return df
        if isinstance(fields, dict):
            df = df.loc[:, [c for c in fields.keys() if c in df.columns]].rename(columns=fields)
        else:
            df = df.loc[:, [c for c in fields if c in df.columns]]
        return df

    # --- Main execution ---
    def execute(self, data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        self.logger.info("Starting MergeTablesPipeline execution")
        left_df = self._select_fields(self._extract(self.left, "left"), self.left_fields)
        right_df = self._select_fields(self._extract(self.right, "right"), self.right_fields)
        self.logger.info(f"Left table shape after field selection: {left_df.shape}")
        self.logger.info(f"Right table shape after field selection: {right_df.shape}")

        # Perform merge
        if self.join_key:
            self.logger.info(f"Merging on join_key: {self.join_key} with how='{self.how}'")
            merged = pd.merge(left_df, right_df, how=self.how, on=self.join_key, suffixes=self.suffixes)
            self.logger.info(f"Merged table shape: {merged.shape}")
        else:
            self.logger.info(f"Merging on left_on: {self.left_on} and right_on: {self.right_on} with how='{self.how}'")
            self.logger.info(f"Left table columns: {left_df.columns.tolist()}")
            self.logger.info(f"Right table columns: {right_df.columns.tolist()}")
            merged = pd.merge(
                left_df, right_df,
                how=self.how,
                left_on=self.left_on,
                right_on=self.right_on,
                suffixes=self.suffixes
            )

        self.logger.info(f"Merged table shape: {merged.shape}")
        # Optional merged transformations
        merged_trans = self.transformations.get("merged") or {}
        if "dropna" in merged_trans and merged_trans["dropna"]:
            merged = merged.dropna()

        return merged

    @classmethod
    def from_config(cls, cfg: dict):
        """Build pipeline from YAML config (compatible with PipelineFactory)."""
        params = cfg.get("params", {}) or {}

        def build_source(spec: dict):
            if not spec:
                raise ValueError("Missing source spec in config")
            extractor_type = spec.get("extractor_type")
            extractor_params = spec.get("extractor_params", {})
            if extractor_type == "table":
                model = extractor_params.get("model")
                if not model:
                    raise ValueError("Table extractor requires 'model'")
                return ExtractorFactory.create_table_extractor(
                    model=model,
                    sample_size=extractor_params.get("sample_size"),
                    order_by=extractor_params.get("order_by"),
                    filters=extractor_params.get("filters"),
                )
            if extractor_type == "dataframe":
                df = extractor_params.get("data")
                if isinstance(df, pd.DataFrame):
                    return df
                raise ValueError("'dataframe' extractor_type requires a pandas.DataFrame")
            raise ValueError(f"Unsupported extractor_type '{extractor_type}'")

        left_source = build_source(params.get("left"))
        right_source = build_source(params.get("right"))


        return cls(
            left_extractor_or_df=left_source,
            right_extractor_or_df=right_source,
            join_key=params.get("join_key"),
            left_on=params.get("left_on"),
            right_on=params.get("right_on"),
            left_fields=params.get("left_fields"),
            right_fields=params.get("right_fields"),
            how=params.get("how", "inner"),
            suffixes=tuple(params.get("suffixes", ("_left", "_right"))),
            transformations=params.get("transformations", {}),
            name=cfg.get("name")
        )