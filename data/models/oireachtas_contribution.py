# data/models/oireachtas_contribution.py
from sqlalchemy import Column, BigInteger, String, ForeignKey
from sqlalchemy.types import UnicodeText, JSON
from sqlalchemy.orm import relationship
from .base import Base


class OireachtasDebateContribution(Base):
    __tablename__ = "oireachtas_debate_contributions"
    __table_args__ = {"schema": "dbo"}

    Id = Column(BigInteger, primary_key=True, autoincrement=True)

    DebateSectionId = Column(
        String(100),
        ForeignKey("dbo.oireachtas_debate_sections.DebateSectionId"),
        index=True,
    )

    SpeakerName = Column(String(255))
    SpeakerURI = Column(String(500))
    SpeakerRole = Column(String(100))

    ContributionType = Column(String(50))
    ContributionText = Column(UnicodeText, nullable=False)

    Sequence = Column(BigInteger)

    RawJSON = Column(JSON, nullable=False)

    DebateSection = relationship("OireachtasDebateSection", back_populates="Contributions")
