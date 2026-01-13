# eda/describe_info_eda.py
from .base import EDAComponent
from logs.logger import get_logger
import os
import pandas as pd
import io


class DescribeInfoEDA(EDAComponent):
    """EDA that writes DataFrame.describe() and DataFrame.info() into CSV files.

    Lightweight: no constructor parameters and relies on the `save_path` provided to `run()`.
    """

    def __init__(self, **kwargs):
        # Accept **kwargs for factory compatibility but ignore them intentionally
        self.logger = get_logger("DescribeInfoEDA")
        self.logger.info("Initialized DescribeInfoEDA")

    def run(self, data, target=None, text_field=None, save_path=None, **kwargs):
        """
        Run the EDA and write outputs to `save_path` as CSV files.

        Parameters:
        - data: pandas DataFrame
        - target: target column (not used here)
        - text_field: text column (not used here)
        - save_path: directory where output files will be written (required)

        Returns a list of paths to the written CSV files: [describe_path, info_path].
        """
        self.logger.info("Running DescribeInfoEDA")

        if save_path is None:
            raise ValueError("save_path must be provided to write EDA outputs")

        os.makedirs(save_path, exist_ok=True)
        describe_path = os.path.join(save_path, "describe.csv")
        info_path = os.path.join(save_path, "info.csv")

        # write describe (include='all' to get non-numeric too)
        try:
            desc = data.describe(include='all')
        except Exception:
            desc = data.describe()

        try:
            # to_csv handles DataFrame directly
            desc.to_csv(describe_path)
        except Exception as e:
            self.logger.warning(f"Failed to write describe() directly to CSV: {e}. Attempting safe conversion.")
            pd.DataFrame(desc).to_csv(describe_path)

        # write info as lines into a CSV with single column 'info'
        buf = io.StringIO()
        data.info(buf=buf)
        info_str = buf.getvalue()
        info_lines = info_str.splitlines() if info_str else ["No info available"]
        df_info = pd.DataFrame({"info": info_lines})
        df_info.to_csv(info_path, index=False)

        self.logger.info(f"EDA outputs written to {describe_path} and {info_path}")
        return [describe_path, info_path]
