# data/models/boards_conceptual_model.py
from sqlalchemy import Column, BigInteger, String, Date, DateTime, Integer, Float
from sqlalchemy.types import UnicodeText
from .base import Base


class BoardsConceptualModel(Base):
    __tablename__ = "boards_conceptual_model"
    __table_args__ = {"schema": "dbo"}

    # DocumentId = Column(BigInteger, primary_key=True)
    # DiscussionId = Column(BigInteger)
    # Type =  Column(String(255), nullable =True)
    # Title =  Column(UnicodeText, nullable=True)
    # OpeningPost = Column(UnicodeText, nullable=True)
    # CategoryId = Column(Integer, nullable = True)
    # PostYear = Column(Integer, nullable = True)
    # PostMonth = Column(Integer, nullable = True)
    # CommentCount = Column(Integer, nullable = True)
    # DateLastComment = Column(DateTime, nullable=True)
    # Document = Column(UnicodeText, nullable=True)


    DocumentId = Column(BigInteger, primary_key=True)
    DocumentDiscussionChainPart = Column(UnicodeText, nullable =True)
    DiscussionId = Column(BigInteger)
    DiscussionTitle = Column(UnicodeText, nullable=True)
    DiscussionBody = Column(UnicodeText, nullable=True)
    DiscussionCategoryId = Column(BigInteger, nullable = True)
    DiscussionDateInserted = Column(DateTime, nullable=True)
    DocumentType =  Column(UnicodeText, nullable =True)
    DocumentStartDate = Column(DateTime, nullable=True)
    DocumentEndDate = Column(DateTime, nullable=True)
    ChainId = Column(BigInteger, nullable = True)
    IsConversationChain = Column(Integer, nullable = True)
    CommentRecords = Column(UnicodeText, nullable=True)
    CommentCount = Column(BigInteger, nullable = True)
    CommentWordCount = Column(BigInteger, nullable = True)
    CommentCharacterCount = Column(BigInteger, nullable = True)
    DurationHours = Column(Float, nullable = True)
    SplitReason = Column(UnicodeText, nullable=True)
    DocumentText = Column(UnicodeText, nullable=True)
    DocumentWordCount = Column(BigInteger, nullable = True)
    DocumentCharacterCount = Column(BigInteger, nullable = True)


    def to_dict(self) -> dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}