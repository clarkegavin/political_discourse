#data/models/boards_discussion.py

from sqlalchemy import Column, BigInteger, String, DateTime, Integer
from sqlalchemy.types import UnicodeText, JSON
from sqlalchemy.orm import relationship
from .base import Base

class BoardsDiscussion(Base):
    __tablename__ = "boards_discussions"
    __table_args__ = {"schema": "dbo"}

    Id = Column(BigInteger, primary_key=True, autoincrement=True)
    DiscussionId = Column(BigInteger, unique=True, nullable=False)
    Type = Column(String(50),)
    Title = Column(String(500), nullable=False) # name
    Body = Column(UnicodeText, nullable=False)
    CategoryId = Column(Integer)
    DateInserted = Column(DateTime)
    DateUpdated = Column(DateTime)
    DateLastComment = Column(DateTime)
    insertUserId = Column(BigInteger)
    UpdateUserId = Column(BigInteger)
    LastUserId = Column(BigInteger)
    Closed = Column(String(10))
    countComments = Column(BigInteger)
    CanonicalURL = Column(String(1000))

