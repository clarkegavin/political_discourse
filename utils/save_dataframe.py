# python
import logging
from typing import Optional
import pandas as pd

logger = logging.getLogger("io_helpers")

def save_df_to_excel(df: pd.DataFrame, path: str, sheet_name: str = "Sheet1", index: bool = False):
    """
    Save DataFrame to Excel file. Requires openpyxl (for .xlsx).
    """
    try:
        df.to_excel(path, sheet_name=sheet_name, index=index)
        logger.info(f"Saved DataFrame to Excel: {path}")
    except Exception as e:
        logger.error(f"Failed to save DataFrame to Excel {path}: {e}")
        raise

def save_df_to_db(
    df: pd.DataFrame,
    table_name: str,
    conn: Optional[str] = None,
    engine=None,
    if_exists: str = "replace",
    index: bool = False,
    chunksize: Optional[int] = None,
):
    """
    Save DataFrame to a database table.
    - Provide either `engine` (SQLAlchemy Engine) or `conn` (SQLAlchemy connection string).
    - Requires sqlalchemy installed.
    """
    try:
        if engine is None:
            if not conn:
                raise ValueError("Either `engine` or `conn` (SQLAlchemy URI) must be provided")
            from sqlalchemy import create_engine
            engine = create_engine(conn)

        df.to_sql(name=table_name, con=engine, if_exists=if_exists, index=index, chunksize=chunksize)
        logger.info(f"Wrote DataFrame to table `{table_name}`")
    except Exception as e:
        logger.error(f"Failed to write DataFrame to DB table {table_name}: {e}")
        raise