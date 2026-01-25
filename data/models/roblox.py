from sqlalchemy import Column, BigInteger, DateTime, String, Float, Boolean, Date as SQLDate
from sqlalchemy.orm import declarative_base
from .base import Base

class RobloxGame(Base):
    """ORM model mapping to dbo.roblox_games_data"""
    #__tablename__ = "roblox_games_data"
    __tablename__ = "roblox_games"
    __table_args__ = {"schema": "dbo"}

    Id = Column("Id", BigInteger, primary_key=True, nullable=False)
    Date = Column("Date", DateTime, nullable=True)
    Active_Users = Column("Active_Users", BigInteger, nullable=True)
    Favorites = Column("Favorites", BigInteger, nullable=True)
    Total_Visits = Column("Total_Visits", String(50), nullable=True)
    Date_Created = Column("Date_Created", SQLDate, nullable=True)
    Last_Updated = Column("Last_Updated", SQLDate, nullable=True)
    Server_Size = Column("Server_Size", BigInteger, nullable=True)
    Genre = Column("Genre", String(100), nullable=True)
    Title = Column("Title", String(100), nullable=True)
    Creator = Column("Creator", String(1000), nullable=True)
    gameID = Column("gameID", BigInteger, nullable=True)
    Category = Column("Category", String(50), nullable=True)
    URL = Column("URL", String(1000), nullable=True)
    Description = Column("Description", String(3000), nullable=True)

    def to_dict(self) -> dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}