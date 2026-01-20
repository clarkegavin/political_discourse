# data/models/oireachtas_debate.py
from sqlalchemy import Column, BigInteger, String, Date, ForeignKey
from sqlalchemy.types import JSON
from sqlalchemy.orm import relationship
from .base import Base


class OireachtasDebateSection(Base):
    __tablename__ = "oireachtas_debate_sections"
    __table_args__ = {"schema": "dbo"}

    Id = Column(BigInteger, primary_key=True, autoincrement=True)
    DebateSectionId = Column(String(100), nullable=False, index=True, unique=True)
    DebateDate = Column(Date, nullable=False)

    QuestionURI = Column(
        String(500),
        ForeignKey("dbo.oireachtas_questions.QuestionURI"),
    )

    Title = Column(String(500))
    RawJSON = Column(JSON, nullable=False)

    Question = relationship("OireachtasQuestion", back_populates="DebateSections")
    Contributions = relationship(
        "OireachtasDebateContribution",
        back_populates="DebateSection",
    )

    def to_dict(self) -> dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}