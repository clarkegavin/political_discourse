# data/models/llm_topics.py
from sqlalchemy import Column, BigInteger, String, Date, DateTime, Integer, Float, Boolean
from .base import Base


class LLMTopicAlignment(Base):
    __tablename__ = "vw_oireachtas_topic_online_alignment"
    __table_args__ = {"schema": "dbo"}

    QuestionId = Column(String(255), nullable=False, primary_key=True)
    Topic = Column(Integer, nullable=False)
    Questioner = Column(String(255), nullable=True)
    QuestionerParty = Column(String(255), nullable=True)
    QuestionerConstituency = Column(String(255), nullable=True)
    QuestionDate = Column(Date)

    TopicTheme = Column(String(255), nullable=True)
    TopicDescription = Column(String(1000), nullable=True)

    IsMatched = Column(Boolean, nullable=True, default=False)
    MaxSimilarity = Column(Float, nullable=True)
    MatchedTargetTopicCount = Column(Integer, nullable=True)





    def to_dict(self) -> dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}