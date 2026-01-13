# pipelines/preprocessing_pipeline.py
from typing import Any, Dict, List, Optional
import pandas as pd
from pipelines.base import Pipeline
from preprocessing.factory import PreprocessorFactory
from logs.logger import get_logger


class PreprocessingPipeline(Pipeline):
    """Pipeline that applies a sequence of text and/or data preprocessors to a dataset.

    Backwards compatible with the older config which separated
    `preprocessors` (text) and `data_preprocessors` (data-level).

    New behaviour: supply a single `preprocessors` list where each entry may include
    an optional `applies_to` flag with values: 'text', 'data', or 'both'.
    The flag defaults to 'text' to preserve existing behaviour.

    Config samples (both forms supported):
    Old (legacy):
    params:
      text_field: 'Description'
      preprocessors:
        - name: stemmer
          params: {language: english}
      data_preprocessors:
        - name: temporal_features
          params:
            date_column: 'ReleaseDate'
            days_since: true
            prefix: 'release'

    New (merged):
    params:
      text_field: 'Description'
      preprocessors:
        - name: stemmer
          params: {language: english}
        - name: temporal_features
          applies_to: data
          params:
            date_column: 'ReleaseDate'
            days_since: true
            prefix: 'release'

    Notes:
    - 'applies_to' defaults to 'text' so existing configs are unaffected.
    - Preprocessors with 'both' will be attempted on the DataFrame first, and
      then (if a text_field is set) against the list of texts. Failures for one
      target won't prevent attempting the other; errors are logged.
    """

    def __init__(
        self,
        preprocessors: Optional[List[Dict[str, Any]]] = None,
        text_field: Optional[str] = None,
        data_preprocessors: Optional[List[Dict[str, Any]]] = None,
        name: Optional[str] = None,
    ):
        # Ensure base initializer runs
        super().__init__(name=name)

        # Accepts either a list of text preprocessors (each is a dict with name/params)
        # and an optional list of data_preprocessors which operate on the full DataFrame.
        # Or a merged `preprocessors` list where each item can specify `applies_to`.
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("Initializing PreprocessingPipeline")

        # Raw inputs
        self.raw_preprocessors = preprocessors or []
        self.legacy_data_preprocessors = data_preprocessors or []
        self.text_field = text_field
        self.logger.info(f"Text field: {self.text_field}")

        # Split into three internal lists according to 'applies_to'
        (
            self.data_preprocessors,
            self.text_preprocessors,
            self.both_preprocessors,
        ) = self._split_preprocessors(self.raw_preprocessors, self.legacy_data_preprocessors)

        self.logger.info(
            f"Configured text preprocessors: {[p.get('name') for p in self.text_preprocessors]}"
        )
        self.logger.info(
            f"Configured data preprocessors: {[p.get('name') for p in self.data_preprocessors]}"
        )
        self.logger.info(
            f"Configured preprocessors (both): {[p.get('name') for p in self.both_preprocessors]}"
        )

    @classmethod
    def from_config(cls, cfg: Dict[str, Any]):
        # Allow PipelineFactory to call from_config if present; it passes the whole pipeline entry
        params = cfg.get("params", cfg)
        preprocessors = params.get("preprocessors", [])
        data_preprocessors = params.get("data_preprocessors", [])
        text_field = params.get("text_field")
        name = cfg.get("name") or params.get("name")
        return cls(
            preprocessors=preprocessors,
            text_field=text_field,
            data_preprocessors=data_preprocessors,
            name=name,
        )

    def _split_preprocessors(self, preprocessors: List[Dict[str, Any]], legacy_data: List[Dict[str, Any]]):
        """Split given preprocessor configs into data/text/both lists.

        - `preprocessors`: new unified list where each item may include `applies_to`
          (defaults to 'text').
        - `legacy_data`: old-style data_preprocessors which are treated as applies_to='data'.

        Returns tuple: (data_list, text_list, both_list)
        """
        data_list = []
        text_list = []
        both_list = []

        # legacy data_preprocessors are data-targeted
        for p in legacy_data:
            item = dict(p)
            item.setdefault("applies_to", "data")
            data_list.append(item)

        for p in preprocessors:
            item = dict(p)
            applies_to = (item.get("applies_to") or "text").lower()
            if applies_to not in ("text", "data", "both"):
                self.logger.warning(f"Unknown applies_to '{applies_to}' for preprocessor {item.get('name')} - defaulting to 'text'")
                applies_to = "text"
                item["applies_to"] = "text"

            if applies_to == "text":
                text_list.append(item)
            elif applies_to == "data":
                data_list.append(item)
            else:  # both
                both_list.append(item)

        return data_list, text_list, both_list

    def execute(self, data: Optional[pd.DataFrame] = None) -> Any:
        """Run data-level preprocessors first, then apply text preprocessors to the text_field.

        Precedence:
        1. data_preprocessors (configured as data)
        2. both_preprocessors applied to DataFrame (if possible)
        3. text preprocessors (configured as text)
        4. both_preprocessors applied to text list (if text_field provided)

        Returns the transformed DataFrame.
        """
        if data is None or not isinstance(data, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame for PreprocessingPipeline")

        df = data.copy()
        # Apply data-level preprocessors (each receives/returns DataFrame)
        for pre_cfg in self.data_preprocessors:
            name = pre_cfg.get("name")
            params = pre_cfg.get("params", {})
            self.logger.info(f"Applying data preprocessor: {name} with params: {params}")
            try:
                pre = PreprocessorFactory.create(name, **params)
            except Exception as e:
                self.logger.warning(f"Could not construct data preprocessor {name}: {e}")
                continue
            try:
                pre.fit(df)
                df = pre.transform(df)
            except Exception as e:
                self.logger.exception(f"Data preprocessor {name} failed: {e}")

        # Apply 'both' preprocessors on the DataFrame when possible
        for pre_cfg in self.both_preprocessors:
            name = pre_cfg.get("name")
            params = pre_cfg.get("params", {})
            self.logger.info(f"Applying 'both' preprocessor to DataFrame: {name} with params: {params}")
            try:
                pre = PreprocessorFactory.create(name, **params)
            except Exception as e:
                self.logger.warning(f"Could not construct 'both' preprocessor {name} for DataFrame application: {e}")
                continue
            try:
                pre.fit(df)
                df = pre.transform(df)
            except Exception as e:
                self.logger.warning(f"Applying preprocessor {name} to DataFrame failed or is unsupported: {e}")

        # Apply text preprocessors on the specified text_field (if provided)
        if self.text_field:
            texts = df[self.text_field].fillna("").tolist()

            for pre_cfg in self.text_preprocessors:
                name = pre_cfg.get("name")
                params = pre_cfg.get("params", {})
                self.logger.info(f"Applying text preprocessor: {name} with params: {params}")
                try:
                    pre = PreprocessorFactory.create(name, **params)
                except Exception as e:
                    self.logger.warning(f"Could not construct text preprocessor {name}: {e}")
                    continue
                try:
                    texts = pre.fit_transform(texts)
                except Exception as e:
                    self.logger.exception(f"Text preprocessor {name} failed: {e}")

            # Apply 'both' preprocessors to the text list (if any)
            for pre_cfg in self.both_preprocessors:
                name = pre_cfg.get("name")
                params = pre_cfg.get("params", {})
                self.logger.info(f"Applying 'both' preprocessor to texts: {name} with params: {params}")
                try:
                    pre = PreprocessorFactory.create(name, **params)
                except Exception as e:
                    self.logger.warning(f"Could not construct 'both' preprocessor {name} for text application: {e}")
                    continue
                try:
                    texts = pre.fit_transform(texts)
                except Exception as e:
                    self.logger.warning(f"Applying preprocessor {name} to texts failed or is unsupported: {e}")

            df = df.copy()
            df[self.text_field] = texts

        self.logger.info("PreprocessingPipeline completed")
        return df


# Note: we intentionally kept execute signature compatible with Pipeline.execute (data optional)
