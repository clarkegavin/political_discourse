#data/sqlalchemy_connector.py
import os
import urllib.parse
from typing import Optional
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from .abstract_connector import DBConnector
from logs.logger import get_logger
from data.models import Base

class SQLAlchemyConnector(DBConnector):
    """
    Concrete connector using SQLAlchemy.

    It prefers a full DB URL provided via the `DB_URL` environment variable.
    If not present it will look for `DB_SERVER` and `DB_NAME` and optional
    `DB_USER`/`DB_PASSWORD` and `DB_DRIVER`. For integrated auth use no user/password.

    Example env vars for Windows integrated auth:
      DB_SERVER=DESKTOP-70D95RL\\SQLEXPRESS
      DB_NAME=steam
      DB_DRIVER=ODBC Driver 17 for SQL Server

    Or set a full URL:
      DB_URL=mssql+pyodbc://<user>:<pw>@<server>/<db>?driver=ODBC+Driver+17+for+SQL+Server
    """

    def __init__(self, db_url: Optional[str] = None, api_key: Optional[str] = None):
        self.logger = get_logger(self.__class__.__name__)

        db_url = db_url or os.getenv("DB_URL")

        self.logger.info(f"Initializing SQLAlchemyConnector with DB_URL: {db_url is not None}")
        if not db_url:
            self.logger.info("DB_URL not set, constructing from individual environment variables")
            server = os.getenv("DB_SERVER")
            database = os.getenv("DB_NAME")
            driver = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
            user = os.getenv("DB_USER")
            password = os.getenv("DB_PASSWORD")
            trusted = os.getenv("DB_TRUSTED_CONNECTION", "yes")

            if not server or not database:
                raise ValueError("Set DB_URL or DB_SERVER and DB_NAME in environment")

            quoted_driver = urllib.parse.quote_plus(driver)
            if user and password:
                db_url = f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={quoted_driver}"
            else:
                # Trusted connection (integrated auth)
                db_url = f"mssql+pyodbc://{server}/{database}?driver={quoted_driver}&trusted_connection={trusted}"

        # Create engine and session factory
        # Use future=True for SQLAlchemy 1.4+ style
        self._engine = create_engine(db_url, future=True)
        self._SessionFactory = sessionmaker(bind=self._engine, future=True)

    def get_engine(self):
        return self._engine

    def get_session(self) -> Session:
        return self._SessionFactory()

    def create_tables(self, base=Base):
        from .models import Base
        try:
            self.logger.info("Creating database tables if they do not exist")
            base.metadata.create_all(self._engine)
        except Exception as e:
            self.logger.error(f"Error creating tables: {e}")
            raise
