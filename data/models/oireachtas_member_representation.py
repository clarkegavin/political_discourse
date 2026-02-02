# data/models/oireachtas_member_representation.py
from sqlalchemy import Column, BigInteger, String, ForeignKey
from sqlalchemy.orm import relationship
from .base import Base


class OireachtasMemberRepresentation(Base):
    __tablename__ = "oireachtas_member_representations"
    __table_args__ = {"schema": "dbo"}

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    membership_id = Column(BigInteger, ForeignKey("dbo.oireachtas_member_memberships.membership_id"), nullable=False)
    representCode = Column(String(100), nullable=True)
    representShowAs = Column(String(255), nullable=True)
    representType = Column(String(100), nullable=True)

    membership = relationship("OireachtasMemberMembership", back_populates="representations")

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
