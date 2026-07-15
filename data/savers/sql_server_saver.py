# data/savers/sql_server_saver.py
from typing import Optional, Literal
import pandas as pd
from sqlalchemy import Table, MetaData
from sqlalchemy import inspect
from logs.logger import get_logger

class SQLServerSaver:
    """Saver that writes a pandas DataFrame to SQL Server using SQLAlchemy connector.

    Usage:
       saver = SQLServerSaver()
       saver.save(df, table_name, connector=connector, if_exists='replace', chunk_size=1000)
    """

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)

    def save(self, df: pd.DataFrame, table_name: str, connector, if_exists: Literal['fail', 'replace', 'append'] = 'replace', chunk_size: int = 1000, schema: Optional[str] = None):
        """Save DataFrame to SQL Server table using SQLAlchemy engine from connector.

        Args:
            df: pandas DataFrame to save (unknown structure)
            table_name: destination table name
            connector: SQLAlchemyConnector instance (provides get_engine())
            if_exists: 'replace' (overwrite), 'append' or 'fail'
            chunk_size: number of rows per chunk write
            schema: optional DB schema
        """
        engine = connector.get_engine()
        self.logger.info(f"Saving DataFrame to table {table_name} (if_exists={if_exists}) with chunk_size={chunk_size}")

        try:
            if if_exists == 'replace':
                # Check existence via inspector and drop the table if present
                inspector = inspect(engine)
                exists = inspector.has_table(table_name, schema=schema)
                if exists:
                    try:
                        meta = MetaData()
                        tbl = Table(table_name, meta, autoload_with=engine, schema=schema)
                        self.logger.info(f"Dropping existing table {table_name}")
                        tbl.drop(engine)
                    except Exception as e:
                        # If reflection failed, fallback to raw DROP TABLE
                        self.logger.warning(f"Could not reflect table for drop, attempting raw DROP: {e}")
                        with engine.connect() as conn:
                            conn.execute(f"DROP TABLE {table_name}"

            # Use pandas to_sql with chunksize

            df.to_sql(name=table_name, con=engine, if_exists=if_exists, index=False, chunksize=chunk_size, schema=schema)
            self.logger.info(f"DataFrame saved successfully to {table_name}")
        except Exception as e:
            self.logger.error(f"Failed to save DataFrame to {table_name}: {e}")
            raise
