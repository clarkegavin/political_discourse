# data/models/oireachtas_topic_summary.py
from sqlalchemy import Column, BigInteger, String, Date, Integer
from .base import Base


class OireachtasTopicSummary(Base):
    __tablename__ = "vw_oireachtas_topic_summary"
    __table_args__ = {"schema": "dbo"}

    QuestionId = Column(BigInteger, primary_key=True)
    Topic = Column(Integer, primary_key=True)
    Questioner = Column(String(255), nullable=True)
    QuestionerParty = Column(String(255), nullable=True)
    QuestionerConstituency = Column(String(255), nullable=True)
    QuestionDate = Column(Date)
    TopicTheme = Column(String(255), nullable=False)
    TopicDescription = Column(String(1000), nullable=True)
    DocumentCount = Column(Integer, nullable=False)



    def to_dict(self) -> dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}