# data/models/oireachtas_question.py
from sqlalchemy import Column, BigInteger, String, Date, DateTime
from sqlalchemy.types import UnicodeText, JSON
from sqlalchemy.sql.sqltypes import Date as SQLDate
from sqlalchemy.orm import relationship
from .base import Base


class OireachtasQuestion(Base):
    __tablename__ = "oireachtas_questions"
    __table_args__ = {"schema": "dbo"}

    Id = Column(BigInteger, primary_key=True, autoincrement=True)
    QuestionURI = Column(String(500), nullable=False, unique=True)
    QuestionNumber = Column(BigInteger)
    QuestionType = Column(String(50))
    QuestionDate = Column(Date)

    QuestionText = Column(UnicodeText, nullable=False)

    AskedBy = Column(String(255))
    AskedByURI = Column(String(500))
    AskedByMemberCode = Column(String(100))

    ToMinister = Column(String(255))
    ToMinisterURI = Column(String(500))
    ToRoleCode = Column(String(100))
    ToRoleType = Column(String(255))

    # AnswerText = Column(UnicodeText, nullable=True)
    # AnswerDate = Column(SQLDate, nullable=True)

    # Optional: extra metadata from API
    House = Column(String(50), nullable=True)
    ChamberType = Column(String(50), nullable=True)
    CommitteeCode = Column(String(50), nullable=True)

    DebateSectionURI = Column(String(500), nullable=True)
    DebateSectionShowAs = Column(String(250), nullable=True)

    AnswerXMLURI = Column(String(500), nullable=True)
    #AnswerXML = Column(UnicodeText, nullable=True)
    AnswerText = Column(UnicodeText, nullable=True)
    AnswerSpeaker = Column(String(255), nullable=True)
    AnswerRecordedTime = Column(DateTime, nullable=True)


    #RawJSON = Column(JSON, nullable=False)

    DebateSections = relationship(
        "OireachtasDebateSection",
        back_populates="Question",
    )

    def to_dict(self) -> dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}