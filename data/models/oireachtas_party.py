# data/models/oireachtas_party.py
from sqlalchemy import Column, BigInteger, String, Integer
from .base import Base


class OireachtasParty(Base):
    __tablename__ = "oireachtas_parties"
    __table_args__ = {"schema": "dbo"}

    Id = Column(BigInteger, primary_key=True, autoincrement=True)

    PartyShowAs = Column(String(255), nullable=True)
    PartyCode = Column(String(100), nullable=True)
    HouseCode = Column(String(100), nullable=True)
    HouseNo = Column(Integer, nullable=True)

    def to_dict(self) -> dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

