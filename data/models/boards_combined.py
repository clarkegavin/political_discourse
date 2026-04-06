# data/models/boards_combined.py

from sqlalchemy import Column, BigInteger, String, DateTime, Integer
from sqlalchemy.types import UnicodeText, JSON
from .base import Base

class BoardsConsolidated(Base):
    __tablename__ = "consolidated_boards"
    __table_args__ = {"schema": "dbo"}

    Id = Column(BigInteger, primary_key=True, autoincrement=True)
    DiscussionId = Column(BigInteger, unique=True, nullable=False)
    DiscussionType = Column(String(50), )
    DiscussionTitle = Column(String(500), nullable=False)  # name
    DiscussionBody = Column(UnicodeText, nullable=False)
    DiscussionCategoryID = Column(Integer)
    DiscussionDateInserted = Column(DateTime)
    DiscussionDateUpdated = Column(DateTime)
    DiscussionDateLastComment = Column(DateTime)
    DiscussionInsertUserID = Column(BigInteger)
    DiscussionUpdateUserID = Column(BigInteger)
    DiscussionLastUserID = Column(BigInteger)
    DiscussionClosed = Column(String(10))
    DiscussionCountComments = Column(BigInteger)
    DiscussionCanonicalUrl = Column(String(1000))

    CommentID = Column(BigInteger, unique=True, nullable=False)
    CommentDiscussionID = Column(BigInteger, index=True)
    CommentName = Column(String(500))
    CommentCategoryID = Column(Integer)
    CommentBody = Column(UnicodeText)
    CommentDateInserted = Column(DateTime)
    CommentDateUpdated = Column(DateTime)
    CommentUpdateUserID = Column(BigInteger)
    CommentScore = Column(Integer)
    CommentDepth = Column(Integer)
    CommentScoreChildComments = Column(Integer)
    CommentCountChildComments = Column(Integer)
    CommentUrl = Column(String(1000))
    # type = Column(String(50))
    # format = Column(String(50))
    # attributes = Column(JSON)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}