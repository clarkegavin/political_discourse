# data/models/oireachtas_member.py
from sqlalchemy import Column, BigInteger, String, Date
from sqlalchemy.orm import relationship
from .base import Base


class OireachtasMember(Base):
    __tablename__ = "oireachtas_members"
    __table_args__ = {"schema": "dbo"}

    pId = Column(String(100), primary_key=True)
    memberCode = Column(String(100), nullable=True)
    firstName = Column(String(255), nullable=True)
    fullName = Column(String(255), nullable=True)
    showAs = Column(String(255), nullable=True)
    gender = Column(String(50), nullable=True)
    dateOfDeath = Column(Date, nullable=True)

    memberships = relationship("OireachtasMemberMembership", back_populates="member")

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
