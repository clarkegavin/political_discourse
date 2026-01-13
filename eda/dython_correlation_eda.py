#eda/dython_correlation_eda.py
from .base import EDAComponent
from logs.logger import get_logger
from visualisations.factory import VisualisationFactory
import os
import pandas as pd
import numpy as np

class DythonCorrelationEDA(EDAComponent):
    """
    EDA component that builds a correlation/association matrix using dython's
    associations and the `CorrelationMatrix` visualisation.

    Params (via kwargs):
    - exclude_columns: list of column names to exclude from the correlation matrix
    - filename: optional output filename (default: "dython_correlation_matrix.png")
    - any other kwargs are forwarded to the visualiser (e.g. annot, cmap, fmt)
    """

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("Initialized DythonCorrelationEDA")

    def run(self, data, target=None, text_field=None, save_path=None, **kwargs):
        if save_path is None:
            save_path = os.getcwd()

        # Validate input
        if data is None or not isinstance(data, pd.DataFrame):
            self.logger.error("DythonCorrelationEDA requires a pandas DataFrame as `data`")
            raise ValueError("DythonCorrelationEDA requires a pandas DataFrame as `data")

        # Limit for performance reasons if specified in kwargs
        max_rows = kwargs.pop('max_rows', data.shape[0])
        limited_data = data.sample(max_rows, random_state=42) if data.shape[0] > max_rows else data.copy()

        # Handle exclude columns
        exclude = kwargs.pop('exclude_columns', []) or []
        if isinstance(exclude, str):
            exclude = [exclude]
        try:
            exclude = list(exclude)
        except Exception:
            exclude = []

        # Handle log transform columns (list or single string)
        log_transform = kwargs.pop('log_transform_columns', []) or []
        if isinstance(log_transform, str):
            log_transform = [log_transform]
        try:
            log_transform = list(log_transform)
        except Exception:
            log_transform = []

        def _norms(x):
            s = '' if x is None else str(x)
            s_raw = s
            s_lower = s.strip().lower()
            s_alnum = ''.join(ch.lower() for ch in s if ch.isalnum())
            s_stripped = s.replace('_', '').replace(' ', '').lower()
            return {s_raw, s_lower, s_alnum, s_stripped}

        exclude_variants = set()
        for ex in exclude:
            try:
                exclude_variants.update(_norms(ex))
            except Exception:
                continue

        # Build list of columns to include (preserve original ordering)
        cols = []
        for c in limited_data.columns:
            try:
                if exclude_variants.intersection(_norms(c)):
                    self.logger.info(f"Excluding column from correlation matrix: {c}")
                    continue
            except Exception:
                pass
            cols.append(c)

        if len(cols) == 0:
            self.logger.error("No columns left to compute correlation matrix after exclusions")
            raise ValueError("No columns left to compute correlation matrix after exclusions")

        df_sub = limited_data[cols]

        # Auto-drop datetime-like columns to avoid comparison issues in dython (float vs Timestamp)
        try:
            datetime_cols = df_sub.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, tz]']).columns.tolist()
            # fallback to any datetime-like detection
            if not datetime_cols:
                datetime_cols = [c for c in df_sub.columns if pd.api.types.is_datetime64_any_dtype(df_sub[c])]
        except Exception:
            datetime_cols = []
        if datetime_cols:
            for c in datetime_cols:
                self.logger.info(f"Dropping datetime-like column from correlation matrix: {c}")
            df_sub = df_sub.drop(columns=datetime_cols)

        # Apply requested log transforms BEFORE plotting
        # We'll match provided names robustly to actual df_sub columns using _norms
        if log_transform:
            # Build normalized map of actual columns -> variants
            col_variants = {c: _norms(c) for c in df_sub.columns}
            for req in log_transform:
                matched = [c for c, variants in col_variants.items() if variants.intersection(_norms(req))]
                if not matched:
                    self.logger.warning(f"Requested log transform for column '{req}' but no matching column found; skipping")
                    continue
                for col in matched:
                    try:
                        ser = df_sub[col]
                        # Try to coerce to numeric if not numeric
                        if not pd.api.types.is_numeric_dtype(ser):
                            ser_num = pd.to_numeric(ser, errors='coerce')
                        else:
                            ser_num = ser.astype(float)

                        if ser_num.dropna().empty:
                            self.logger.warning(f"Column '{col}' has no numeric values after coercion; skipping log transform")
                            continue

                        minv = ser_num.min()
                        if pd.isna(minv):
                            self.logger.warning(f"Column '{col}' min is NaN; skipping log transform")
                            continue
                        if minv <= 0:
                            shift = 1.0 - float(minv)
                            self.logger.info(f"Shifting column '{col}' by {shift} before log1p (min={minv})")
                            ser_trans = np.log1p(ser_num + shift)
                        else:
                            ser_trans = np.log1p(ser_num)

                        # Assign transformed values back (keep index alignment) without triggering SettingWithCopyWarning
                        df_sub.loc[:, col] = ser_trans
                        self.logger.info(f"Applied log1p transform to column '{col}'")
                    except Exception as e:
                        self.logger.warning(f"Failed to apply log transform to '{col}': {e}")

        # Filepath
        filename = kwargs.pop('filename', None) or "dython_correlation_matrix.png"
        filepath = os.path.join(save_path, filename)

        # Instantiate visualiser and plot. Forward remaining kwargs.
        viz = VisualisationFactory.get_visualisation('dython_correlation_matrix', title=kwargs.get('title', 'Correlation Matrix'), output_dir=save_path, **kwargs)
        fig, ax = viz.plot(df_sub, save_path=filepath, **kwargs)
        self.logger.info(f"Saved correlation matrix to {filepath}")
        return filepath
