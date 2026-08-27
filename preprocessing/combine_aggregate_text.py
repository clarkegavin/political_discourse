from .base import Preprocessor
from logs.logger import get_logger
import pandas as pd
from typing import List, Optional


class CombineOrAggregateText(Preprocessor):
    """
    Preprocessor to create a new text column by either:
    1) Combining fields row-wise
    2) Aggregating fields group-wise (deduplicated)

    Exactly one mode must be used.

    Yaml config example:
    - name: combine_text
      type: preprocessing.combine_or_aggregate_text.CombineOrAggregateText
      params:
        fields_to_combine: ["DiscussionBody", "CommentBody"]
        combined_field_name: "CombinedBody"

    - name: aggregate_text
      type: preprocessing.combine_or_aggregate_text.CombineOrAggregateText
      params:
        fields_to_aggregate: ["DiscussionBody", "CommentBody"]
        aggregated_field_name: "AggregatedBody"
        group_by_field: "DiscussionID"

    """

    def __init__(
        self,
        fields_to_combine: Optional[List[str]] = None,
        combined_field_name: Optional[str] = None,
        fields_to_aggregate: Optional[List[str]] = None,
        aggregated_field_name: Optional[str] = None,
        group_by_field: Optional[str] = None,
        field_prefixes: Optional[dict] = None,
    ):
        self.logger = get_logger(self.__class__.__name__)

        self.fields_to_combine = fields_to_combine
        self.combined_field_name = combined_field_name

        self.fields_to_aggregate = fields_to_aggregate
        self.aggregated_field_name = aggregated_field_name
        self.group_by_field = group_by_field
        self.field_prefixes = field_prefixes or {}

        self.logger.info(
            f"Initialized CombineOrAggregateText with "
            f"combine={fields_to_combine}, aggregate={fields_to_aggregate}"
        )

    # ---------------------------------------------------------------------
    def fit(self, X):
        return self

    # ---------------------------------------------------------------------
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Expected pandas DataFrame")

        df = df.copy()

        # Enforce mutually exclusive modes
        if self.combined_field_name and self.aggregated_field_name:
            raise ValueError("Only one of combined_field_name or aggregated_field_name can be set")

        # -----------------------------------------------------------------
        # COMBINE MODE (row-wise)
        # -----------------------------------------------------------------
        if self.combined_field_name:
            if not self.fields_to_combine:
                raise ValueError(
                    "fields_to_combine must be provided for combine mode"
                )

            self.logger.info(
                f"Running COMBINE mode → {self.combined_field_name}"
            )

            cols = list(self.fields_to_combine)

            missing = [
                col for col in cols
                if col not in df.columns
            ]

            if missing:
                raise ValueError(
                    f"Fields to combine not found in DataFrame: {missing}"
                )

            parts = []

            for col in cols:
                prefix = self.field_prefixes.get(col)

                values = (
                    df[col]
                    .fillna("")
                    .astype(str)
                    .str.replace(r"\s+", " ", regex=True)
                    .str.strip()
                )

                if prefix:
                    values = prefix + ": " + values

                parts.append(values)

            df[self.combined_field_name] = (
                pd.concat(parts, axis=1)
                .agg(" ".join, axis=1)
                .str.replace(r"\s+", " ", regex=True)
                .str.strip()
            )

            return df

        # -----------------------------------------------------------------
        # AGGREGATE MODE (group-wise)
        # -----------------------------------------------------------------
        elif self.aggregated_field_name:
            if not self.fields_to_aggregate:
                raise ValueError("fields_to_aggregate must be provided for aggregate mode")

            self.logger.info(
                f"Running AGGREGATE mode → {self.aggregated_field_name} grouped by {self.group_by_field}"
            )

            cols = list(self.fields_to_aggregate)

            def _aggregate_group(gdf):
                parts = []
                for col in cols:
                    if col not in gdf.columns:
                        continue
                    vals = gdf[col].dropna().astype(str).unique()
                    if len(vals) > 0:
                        parts.append(" ".join(vals))
                return " ".join(parts).strip()

            if self.group_by_field:
                if self.group_by_field not in df.columns:
                    raise ValueError(f"{self.group_by_field} not in DataFrame")

                aggregated = (
                    df.groupby(self.group_by_field)
                    .apply(_aggregate_group)
                    .reset_index(name=self.aggregated_field_name)
                )

                return aggregated

            else:
                # Global aggregation → single row
                text = _aggregate_group(df)
                return pd.DataFrame({self.aggregated_field_name: [text]})

        else:
            raise ValueError("Must specify either combine or aggregate mode")

    # Alias
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.transform(df)