# eda/info_eda.py
from .base import EDAComponent
from logs.logger import get_logger
import os
import pandas as pd
import io


class InfoEDA(EDAComponent):
    """EDA that writes DataFrame.info() into a CSV file.

    Lightweight: no constructor params; relies on save_path passed to run().
    """

    def __init__(self, **kwargs):
        self.logger = get_logger("InfoEDA")
        self.logger.info("Initialized InfoEDA")

    def run(self, data, target=None, text_field=None, save_path=None, **kwargs):
        self.logger.info("Running InfoEDA")

        if save_path is None:
            raise ValueError("save_path must be provided to write EDA outputs")

        os.makedirs(save_path, exist_ok=True)
        out_path = os.path.join(save_path, "data_info.csv")

        buf = io.StringIO()
        data.info(buf=buf)
        info_str = buf.getvalue()
        info_lines = info_str.splitlines() if info_str else ["No info available"]
        df_info = pd.DataFrame({"info": info_lines})

        df_info.to_csv(out_path, index=False)

        self.logger.info(f"Info EDA output written to {out_path}")
        return out_path
