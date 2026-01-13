"""Quick test harness that uses an in-memory SQLite DB to exercise the extractor.

This creates the table, inserts one row, then uses the extractor to fetch the row.
Run: python -m pytest tests/test_harness.py OR python tests/test_harness.py
"""
import sys
from pathlib import Path

# Ensure project root is on sys.path so `from data import ...` works when running this file directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from data.factory import ExtractorFactory
from data.models import Base, RobloxGame
from data.sqlalchemy_connector import SQLAlchemyConnector


def setup_in_memory_db():
    engine = create_engine("sqlite:///:memory:", future=True)
    # SQLite does not support schemas like 'dbo' used for SQL Server. Remove schema on all tables
    for tbl in list(Base.metadata.tables.values()):
        tbl.schema = None

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, future=True)
    # insert a row
    with Session() as session:
        game = RobloxGame(
            Id=1,
            Date=None,
            Active_Users=100,
            Favorites=10,
            Total_Visits="1000",
            Date_Created=None,
            Last_Updated=None,
            Server_Size=20,
            Genre="Adventure",
            Title="Test Game",
            Creator="Tester",
            gameID=123456,
            Category="Fun",
            URL="https://example.com",
            Description="A test game",
        )
        session.add(game)
        session.commit()
    return engine


def run_harness():
    engine = setup_in_memory_db()
    # Create a connector that points to the same in-memory engine by passing the engine directly
    # We'll create a lightweight connector for the engine created above.
    class _EngineConnector(SQLAlchemyConnector):
        def __init__(self, engine):
            self._engine = engine
            from sqlalchemy.orm import sessionmaker

            self._SessionFactory = sessionmaker(bind=self._engine, future=True)

        def get_engine(self):
            return self._engine

        def get_session(self):
            return self._SessionFactory()

    connector = _EngineConnector(engine)
    extractor = ExtractorFactory.create_roblox_extractor(connector=connector)

    rows = extractor.fetch_all()
    print('Rows fetched:', rows)

    single = extractor.fetch_by_game_id(123456)
    print('Single fetched:', single)


if __name__ == '__main__':
    run_harness()
