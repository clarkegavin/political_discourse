# data/models/boards_comment.py

from sqlalchemy import Column, BigInteger, String, DateTime, Integer
from sqlalchemy.types import UnicodeText, JSON
from .base import Base

class BoardsComment(Base):
    __tablename__ = "boards_comments"
    __table_args__ = {"schema": "dbo"}

    Id = Column(BigInteger, primary_key=True, autoincrement=True)
    commentID = Column(BigInteger, unique=True, nullable=False)
    discussionID = Column(BigInteger, index=True)
    parentRecordType = Column(String(100))
    parentRecordID = Column(BigInteger)
    name = Column(String(500))
    categoryID = Column(Integer)
    body = Column(UnicodeText)
    dateInserted = Column(DateTime)
    dateUpdated = Column(DateTime)
    updateUserID = Column(BigInteger)
    score = Column(Integer)
    depth = Column(Integer)
    scoreChildComments = Column(Integer)
    countChildComments = Column(Integer)
    url = Column(String(1000))
    type = Column(String(50))
    format = Column(String(50))
    attributes = Column(JSON)

