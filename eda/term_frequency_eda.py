from .base import EDAComponent
from logs.logger import get_logger
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from data.savers.factory import DataSaverFactory
from data.sqlalchemy_connector import SQLAlchemyConnector

class TermFrequencyEDA(EDAComponent):
    """
    EDA component to compute term frequencies from one or more text fields.

    Constructor params (can be provided via YAML params when creating the EDA):
    - fields_to_combine: optional list of column names to combine
    - combined_field_name: optional name for the new combined column (defaults to '__combined__')
    - target_field: the column name on which to compute term frequencies
    """

    def __init__(self, fields_to_combine=None, combined_field_name='__combined__', target_field=None):
        self.logger = get_logger("TermFrequencyEDA")
        self.fields_to_combine = fields_to_combine
        self.combined_field_name = combined_field_name or '__combined__'
        self.target_field = target_field
        self.logger.info(f"Initialized TermFrequencyEDA; fields_to_combine={fields_to_combine}, combined_field_name={self.combined_field_name}, target_field={target_field}")

    def run(self, data, target=None, text_field=None, save_path=None, **kwargs):
        """
        Compute term frequencies and return a pandas DataFrame with columns ['term', 'frequency'] sorted by descending frequency.

        Parameters:
        - data: pandas DataFrame
        - target: not used here (kept for compatibility)
        - text_field: not used here (kept for compatibility)
        - kwargs: may contain overrides for fields_to_combine, combined_field_name, target_field and optional saver params
        """
        # Resolve configuration: instance values take precedence; kwargs override when provided
        fields_to_combine = kwargs.get('fields_to_combine', self.fields_to_combine)
        combined_field_name = kwargs.get('combined_field_name', self.combined_field_name) or '__combined__'
        target_field = kwargs.get('target_field', self.target_field)

        # Saver params (optional)
        saver_name = kwargs.get('saver_name')
        table_name = kwargs.get('table_name')
        if_exists = kwargs.get('if_exists', 'replace')
        chunk_size = kwargs.get('chunk_size', 1000)
        schema = kwargs.get('schema')
        connector_params = kwargs.get('connector_params', {}) or {}

        # Validate data
        if not isinstance(data, pd.DataFrame):
            self.logger.error("`data` must be a pandas DataFrame")
            raise ValueError("`data` must be a pandas DataFrame")

        df = data.copy()

        # If fields_to_combine provided and is non-empty, combine them into combined_field_name
        if fields_to_combine:
            if isinstance(fields_to_combine, str):
                fields_to_combine = [fields_to_combine]
            try:
                fields_to_combine = list(fields_to_combine)
            except Exception:
                self.logger.error("`fields_to_combine` must be a list or iterable of column names")
                raise

            # Fill missing values with empty strings and concatenate with space
            for col in fields_to_combine:
                if col not in df.columns:
                    self.logger.warning(f"Column '{col}' not found in DataFrame; treating as empty string")
                    df[col] = ''
                else:
                    df[col] = df[col].fillna('')

            # Create combined column
            df[combined_field_name] = df[fields_to_combine].astype(str).agg(' '.join, axis=1)
            target_col = combined_field_name
        else:
            # No combination requested: use target_field
            if not target_field:
                self.logger.error("`target_field` must be provided when no `fields_to_combine` are specified")
                raise ValueError("`target_field` must be provided when no `fields_to_combine` are specified")
            target_col = target_field
            if target_col not in df.columns:
                self.logger.error(f"Target column '{target_col}' not found in DataFrame")
                raise ValueError(f"Target column '{target_col}' not found in DataFrame")

        # Ensure the target column exists; fill NA with empty strings
        if target_col not in df.columns:
            self.logger.error(f"Computed target column '{target_col}' not found in DataFrame")
            raise ValueError(f"Computed target column '{target_col}' not found in DataFrame")

        texts = df[target_col].astype(str).fillna('')

        # Use CountVectorizer with basic tokenization and lowercase conversion
        try:
            vec = CountVectorizer(lowercase=True)
            X = vec.fit_transform(texts)
        except Exception as e:
            self.logger.error(f"Failed to vectorize text: {e}")
            raise

        # Sum counts across all documents to get term frequencies
        import numpy as _np
        freqs = _np.ravel(X.sum(axis=0))
        terms = vec.get_feature_names_out()

        tf_df = pd.DataFrame({'term': terms, 'frequency': freqs})
        tf_df['relative_freq'] = tf_df['frequency'] / tf_df['frequency'].sum()
        tf_df['cumulative_freq'] = tf_df['relative_freq'].cumsum()
        tf_df = tf_df.sort_values('frequency', ascending=False).reset_index(drop=True)

        # Optionally save the term-frequency DataFrame using the DataSaverFactory (reuse DataSaverPipeline behaviour)
        if table_name:
            # Use provided saver_name or default to 'sql_server'
            saver_name = saver_name or 'sql_server'
            self.logger.info(f"Saving term-frequency DataFrame to table '{table_name}' using saver '{saver_name}'")
            saver = DataSaverFactory.get_saver(saver_name)
            if saver is None:
                self.logger.error(f"Data saver '{saver_name}' not found in DataSaverFactory")
                raise ValueError(f"Data saver '{saver_name}' not found in DataSaverFactory")

            # Instantiate connector
            try:
                connector = SQLAlchemyConnector(**connector_params) if connector_params is not None else SQLAlchemyConnector()
            except Exception as e:
                self.logger.error(f"Failed to initialize SQLAlchemyConnector: {e}")
                raise

            try:
                saver.save(
                    df=tf_df,
                    table_name=table_name,
                    connector=connector,
                    if_exists=if_exists,
                    chunk_size=chunk_size,
                    schema=schema,
                )
            except Exception as e:
                self.logger.error(f"Failed to save term-frequency DataFrame: {e}")
                raise

        return tf_df
