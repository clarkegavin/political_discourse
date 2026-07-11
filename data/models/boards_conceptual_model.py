# data/models/boards_conceptual_model.py
from sqlalchemy import Column, BigInteger, String, Date, DateTime, Integer
from sqlalchemy.types import UnicodeText
from .base import Base


class BoardsConceptualModel(Base):
    __tablename__ = "boards_conceptual_model"
    __table_args__ = {"schema": "dbo"}

    DocumentId = Column(BigInteger, primary_key=True)
    DiscussionId = Column(BigInteger)
    Type =  Column(String(255), nullable =True)
    Title =  Column(UnicodeText, nullable=True)
    OpeningPost = Column(UnicodeText, nullable=True)
    CategoryId = Column(Integer, nullable = True)
    PostYear = Column(Integer, nullable = True)
    PostMonth = Column(Integer, nullable = True)
    CommentCount = Column(Integer, nullable = True)
    DateLastComment = Column(DateTime, nullable=True)
    Document = Column(UnicodeText, nullable=True)


    def to_dict(self) -> dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}