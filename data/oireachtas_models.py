# #data/oireachtas_models.py
# from sqlalchemy import Column, BigInteger, DateTime, String, Float, Boolean, Date as SQLDate
# from sqlalchemy.orm import declarative_base
# from sqlalchemy.types import UnicodeText
# from sqlalchemy.dialects.mssql import JSON
# from sqlalchemy.orm import relationship
#
# Base = declarative_base()
#
# class OireachtasQuestion(Base):
#     __tablename__ = "oireachtas_questions"
#     __table_args__ = {"schema": "dbo"}
#
#     Id = Column(BigInteger, primary_key=True, autoincrement=True)
#
#     # Core fields
#     QuestionURI = Column(String(500), nullable=False, unique=True)
#     QuestionNumber = Column(BigInteger, nullable=True)
#     QuestionType = Column(String(50), nullable=True)
#     QuestionDate = Column(SQLDate, nullable=True)
#
#     QuestionText = Column(UnicodeText, nullable=False)
#     AskedBy = Column(UnicodeText, nullable=True)
#     ToMinister = Column(UnicodeText, nullable=True)
#     AnswerText = Column(UnicodeText, nullable=True)
#     AnswerDate = Column(SQLDate, nullable=True)
#
#     # Optional: extra metadata from API
#     House = Column(UnicodeText, nullable=True)
#     ChamberType = Column(String(50), nullable=True)
#     CommitteeCode = Column(String(50), nullable=True)
#     DebateSectionURI = Column(String(500), nullable=True)
#     DebateSectionShowAs = Column(UnicodeText, nullable=True)
#
#     # Store full payload
#     RawJSON = Column(JSON, nullable=False)
#
#     # relationship
#     DebateSections = relationship(
#         "OireachtasDebateSection",
#         back_populates="Question",
#         cascade="all, delete-orphan",
#     )