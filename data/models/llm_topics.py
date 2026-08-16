# data/models/llm_topics.py
from sqlalchemy import Column, BigInteger, String, Date, DateTime, Integer, Float
from .base import Base


class LLMTopic(Base):
    __tablename__ = "vw_topic_llm"
    __table_args__ = {"schema": "dbo"}

    Identifier = Column(String(255), nullable=False, primary_key=True)
    Dataset = Column(String(50), nullable=False)
    TopicId = Column(Integer, nullable=False)
    TopicLabel = Column(String(255), nullable=False)
    TopicCount = Column(Integer, nullable=False)
    TopicTheme = Column(String(255), nullable=False)
    TopicDescription = Column(String(1000), nullable=True)
    TopicConfidence = Column(Float, nullable=True)
    LLMProvider = Column(String(50), nullable=True)
    LLMModel = Column(String(50), nullable=True)
    LLMPromptVersion = Column(String(255), nullable=True)
    LLMGeneratedAt = Column(DateTime, nullable=True)





    def to_dict(self) -> dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}