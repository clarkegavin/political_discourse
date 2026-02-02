# data/models/oireachtas_member_party_membership.py
from sqlalchemy import Column, BigInteger, String, Date, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from .base import Base


class OireachtasMemberPartyMembership(Base):
    __tablename__ = "oireachtas_member_party_memberships"
    __table_args__ = (
        UniqueConstraint(
            "membership_id",
            "partyCode",
            "party_start_date",
            name="uq_membership_party_start",
        ),
        {"schema": "dbo"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    membership_id = Column(BigInteger, ForeignKey("dbo.oireachtas_member_memberships.membership_id"), nullable=False)
    partyCode = Column(String(100), nullable=True)
    party_showAs = Column(String(255), nullable=True)
    party_start_date = Column(Date, nullable=True)
    party_end_date = Column(Date, nullable=True)

    membership = relationship("OireachtasMemberMembership", back_populates="party_memberships")

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
