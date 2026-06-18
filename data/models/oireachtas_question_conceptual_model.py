# data/models/oireachtas_question_conceptualModel.py
from sqlalchemy import Column, BigInteger, String, Date, DateTime
from sqlalchemy.types import UnicodeText
from .base import Base


class OireachtasQuestionConceptualModel(Base):
    __tablename__ = "vw_oireacthas_questions_conceptual_model"
    __table_args__ = {"schema": "dbo"}

    QuestionId = Column(BigInteger, primary_key=True)
    QuestionNumber = Column(BigInteger)
    QuestionType = Column(String(50))
    QuestionDate = Column(Date)

    QuestionText = Column(UnicodeText, nullable=False)

    Questioner = Column(String(255), nullable =True)
    QuestionCategory = Column(String(250), nullable=True)
    QuestionerParty = Column(String(255), nullable=True)
    QuestionerConstituency = Column(String(255), nullable=True)

    AnswerText = Column(UnicodeText, nullable=True)
    AnsweredBy = Column(String(255), nullable=True)
    AnswerRecordedTime = Column(DateTime, nullable=True)
    AskedOfMinister = Column(String(255), nullable=True)


    def to_dict(self) -> dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}