# data/models/oireachtas_member_membership.py
from sqlalchemy import Column, BigInteger, String, Integer, Date, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from .base import Base


class OireachtasMemberMembership(Base):
    __tablename__ = "oireachtas_member_memberships"
    __table_args__ = (
        UniqueConstraint(
            "pId",
            "houseCode",
            "houseNo",
            "membership_uri",
            name="uq_member_house_membership_uri",
        ),
        {"schema": "dbo"},
    )

    membership_id = Column(BigInteger, primary_key=True, autoincrement=True)
    pId = Column(String(100), ForeignKey("dbo.oireachtas_members.pId"), nullable=False)
    houseCode = Column(String(100), nullable=True)
    houseNo = Column(Integer, nullable=True)
    membership_start_date = Column(Date, nullable=True)
    membership_end_date = Column(Date, nullable=True)
    membership_uri = Column(String(500), nullable=True)

    member = relationship("OireachtasMember", back_populates="memberships")
    party_memberships = relationship(
        "OireachtasMemberPartyMembership",
        back_populates="membership",
        cascade="all, delete-orphan",
    )
    representations = relationship(
        "OireachtasMemberRepresentation",
        back_populates="membership",
        cascade="all, delete-orphan",
    )
    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

