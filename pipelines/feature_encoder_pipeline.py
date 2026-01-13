# pipelines/feature_encoder_pipeline.py
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
from logs.logger import get_logger
from pipelines.base import Pipeline
from encoders.factory import EncoderFactory

class FeatureEncoderPipeline(Pipeline):
    """Encodes features (X columns) using configured encoders.

    YAML params example (pipeline-level `params`):

    params:
      encoders:
        - name: onehot
          columns: ['Category']
          params: {}
        - name: multihot
          columns: ['Platforms']
          params: {sep: ','}
        - name: sklearn_label
          columns: ['Type']

    The pipeline will create one encoder instance per (encoder, column) pair.
    If an encoder's transform returns a DataFrame (e.g., one-hot / multi-hot),
    the new columns are concatenated into the output DataFrame and the original
    source column is dropped.
    """

    def __init__(self, encoders: List[Tuple[Any, List[str]]]):
        """
        encoders: List of (encoder_instance, [columns]) pairs.
        encoder_instance must implement fit/transform.
        """
        self._encoders = encoders
        self.logger = get_logger(self.__class__.__name__)

    @classmethod
    def from_config(cls, cfg: Dict[str, Any]) -> "FeatureEncoderPipeline":
        # Support being passed either the full pipeline entry or just params
        params = cfg.get("params", cfg)
        raw_entries = params.get("encoders") or ([params.get("encoder")] if params.get("encoder") else [])
        encoders: List[Tuple[Any, List[str]]] = []

        for entry in raw_entries:
            if not entry:
                continue
            name = entry.get("name")
            cols = entry.get("columns") or entry.get("cols") or entry.get("columns_to_encode")
            enc_params = entry.get("params", {}) or {}

            if cols is None:
                # if no columns provided, skip this encoder entry
                continue
            if isinstance(cols, str):
                cols = [cols]

            for col in cols:
                encoder_inst = EncoderFactory.create(name, **enc_params)
                encoders.append((encoder_inst, [col]))

        return cls(encoders)

    def fit(self, df: pd.DataFrame) -> "FeatureEncoderPipeline":
        for encoder, cols in self._encoders:
            for col in cols:
                if col in df.columns:
                    try:
                        encoder.fit(df[col])
                    except Exception as e:
                        self.logger.warning(f"Encoder fit failed for column '{col}': {e}")
        return self

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for encoder, cols in self._encoders:
            for col in cols:
                if col not in out.columns:
                    self.logger.info(f"Column '{col}' not found in DataFrame; skipping encoder {encoder.__class__.__name__}")
                    continue
                try:
                    transformed = encoder.transform(out[col])
                except Exception as e:
                    self.logger.warning(f"Encoder transform failed for column '{col}': {e}")
                    continue

                # If transform returned a pandas DataFrame, concatenate and drop original column
                if isinstance(transformed, pd.DataFrame):
                    # preserve index
                    transformed.index = out.index
                    out = pd.concat([out.drop(columns=[col]), transformed], axis=1)
                    self.logger.info(f"Replaced column '{col}' with {transformed.shape[1]} encoded columns from {encoder.__class__.__name__}")
                elif isinstance(transformed, pd.Series):
                    out[col] = transformed
                else:
                    # numpy array or other iterable
                    import numpy as _np
                    arr = _np.asarray(transformed)
                    if arr.ndim == 1:
                        out[col] = arr
                    else:
                        # 2D array: create column names using encoder class and position
                        n_new = arr.shape[1]
                        new_cols = [f"{col}__enc_{i}" for i in range(n_new)]
                        df_new = pd.DataFrame(arr, index=out.index, columns=new_cols)
                        out = pd.concat([out.drop(columns=[col]), df_new], axis=1)
                        self.logger.info(f"Replaced column '{col}' with {n_new} encoded columns (ndarray) from {encoder.__class__.__name__}")
        return out

    def fit_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.fit(df)
        return self.transform(df)

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or not isinstance(df, pd.DataFrame):
            raise ValueError(f"Input must be a DataFrame for FeatureEncoderPipeline instead of {type(df)}")
        cols = [c for _, cols in self._encoders for c in cols]
        self.logger.info(f"Encoding features: {cols}")
        return self.fit_transform(df)
