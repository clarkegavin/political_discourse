#data/models.py
from sqlalchemy import Column, BigInteger, DateTime, String, Float, Boolean, Date as SQLDate
from sqlalchemy.orm import declarative_base
from sqlalchemy.types import UnicodeText

Base = declarative_base()


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

class SteamGame(Base):
    """ORM model mapping to dbo.steam_games table"""
    __tablename__ = "steam_games"
    __table_args__ = {"schema": "dbo"}

    # Core identifiers
    Id = Column("Id", BigInteger, primary_key=True, autoincrement=True)
    AppID = Column("AppID", BigInteger, nullable=False)
    #Name = Column("Name", String(255), nullable=False)
    Name = Column("Name", UnicodeText, nullable=False)
    Type = Column("Type", String(50), nullable=True)
    Is_Free = Column("Is_Free", String(10), nullable=True)  # store as 'True'/'False' for consistency

    # Descriptions
    # Short_Description = Column("Short_Description", String(3000), nullable=True)
    # Detailed_Description = Column("Detailed_Description", String(3000), nullable=True)
    # About_The_Game = Column("About_The_Game", String(3000), nullable=True)

    # Developers / Publishers / URLs
    Developers = Column("Developers", UnicodeText, nullable=True)
    Publishers = Column("Publishers", UnicodeText, nullable=True)
    URL = Column("URL", String(1000), nullable=True)

    # Platforms / Categories / Genres / Tags
    Platforms = Column("Platforms", String(100), nullable=True)
    Categories = Column("Categories", String(500), nullable=True)
    Genres = Column("Genres", String(500), nullable=True)
    Tags = Column("Tags", String(500), nullable=True)
    Supported_Languages = Column("Supported_Languages", String(4000), nullable=True)

    # Pricing
    Price_Currency = Column("Price_Currency", String(10), nullable=True)
    Price_Initial = Column("Price_Initial", BigInteger, nullable=True)
    Price_Final = Column("Price_Final", BigInteger, nullable=True)
    Price_Discount_Percent = Column("Price_Discount_Percent", BigInteger, nullable=True)

    # Reviews / Ratings
    Total_Reviews = Column("Total_Reviews", BigInteger, nullable=True)
    Total_Positive = Column("Total_Positive", BigInteger, nullable=True)
    Total_Negative = Column("Total_Negative", BigInteger, nullable=True)
    Review_Score = Column("Review_Score", BigInteger, nullable=True)
    Review_Score_Desc = Column("Review_Score_Desc", String(100), nullable=True)
    Recommendations = Column("Recommendations", BigInteger, nullable=True)
    Metacritic_Score = Column("Metacritic_Score", BigInteger, nullable=True)
    Metacritic_URL = Column("Metacritic_URL", String(1000), nullable=True)

    # Release / Content
    Release_Date = Column("Release_Date", DateTime, nullable=True)
    Content_Descriptors = Column("Content_Descriptors", String(200), nullable=True)

    #Current Players
    Current_Players = Column("Current_Players", BigInteger, nullable=True)
    Current_Players_Last_Updated = Column("Current_Players_Last_Updated", DateTime, nullable=True)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}