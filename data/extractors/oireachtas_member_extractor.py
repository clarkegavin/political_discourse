# data/extractors/oireachtas_member_extractor.py
from typing import List, Dict
from dateutil.parser import isoparse
from sqlalchemy.orm import Session
from logs.logger import get_logger
from data.models.oireachtas_member import OireachtasMember
from data.models.oireachtas_member_membership import OireachtasMemberMembership
from data.models.oireachtas_member_party_membership import OireachtasMemberPartyMembership
from data.models.oireachtas_member_representation import OireachtasMemberRepresentation


class OireachtasMemberExtractor:
    def __init__(self, connector, chunk_size: int = 100):
        self.connector = connector
        self.chunk_size = chunk_size
        self.logger = get_logger(self.__class__.__name__)

    def _parse_date(self, value):
        if not value:
            return None
        try:
            return isoparse(value).date()
        except Exception:
            return None

    def save_data(self, records: List[Dict]):
        """Normalize and save member data into normalized tables in chunks.

        The method is idempotent: it upserts members (by pId) and avoids
        duplicating membership rows by checking for existing membership_uri
        where available.
        """
        session: Session = self.connector.get_session()
        try:
            for i in range(0, len(records), self.chunk_size):
                chunk = records[i:i + self.chunk_size]
                self._save_chunk(session, chunk)
        finally:
            session.close()

    def _save_chunk(self, session: Session, records: List[Dict]):
        """Insert members, memberships, party memberships and representations.

        Strategy:
          - Upsert core member rows by pId
          - For each membership in the member record, check existence by membership_uri
            (if provided) or by (pId, houseCode, houseNo, membership_start_date)
          - Insert membership -> retrieve membership_id
          - Insert party memberships and representations linked to membership_id
        """
        # Pre-load existing members pIds in this chunk to know which to insert/update
        pids = [self._member_pid(r) for r in records if self._member_pid(r) is not None]
        existing_members: set[str] = set()
        if pids:
            rows = session.query(OireachtasMember.pId).filter(
                OireachtasMember.pId.in_(pids)
            ).all()
            existing_members = {r[0] for r in rows}

        for r in records:
            member_node = r.get("member") or r
            pId = self._member_pid(member_node)
            if pId is None:
                self.logger.warning("Skipping member record with missing pId")
                continue
            # Upsert member core
            member = session.get(OireachtasMember, pId)

            if member:
                member.memberCode = member_node.get("memberCode")
                member.firstName = member_node.get("firstName")
                member.fullName = member_node.get("fullName")
                member.showAs = member_node.get("showAs")
                member.gender = member_node.get("gender")
                member.dateOfDeath = self._parse_date(member_node.get("dateOfDeath"))
            else:
                member = OireachtasMember(
                    pId=pId,
                    memberCode=member_node.get("memberCode"),
                    firstName=member_node.get("firstName"),
                    fullName=member_node.get("fullName"),
                    showAs=member_node.get("showAs"),
                    gender=member_node.get("gender"),
                    dateOfDeath=self._parse_date(member_node.get("dateOfDeath")),
                )
                session.add(member)

            session.flush()

            # Process memberships (list)
            memberships = member_node.get("memberships") or member_node.get("membership") or []
            for m in memberships:
                # membership-level fields
                membership = m.get("membership", {})
                house = membership.get("house") or {}
                membership_uri = membership.get("uri") or membership.get("membershipUri")

                date_range = membership.get("dateRange", {})
                membership_start = self._parse_date(date_range.get("start"))
                membership_end = self._parse_date(date_range.get("end"))
                # Check if membership already exists: prefer membership_uri when present
                existing_membership = None
                if membership_uri:
                    existing_membership = session.query(OireachtasMemberMembership).filter_by(membership_uri=membership_uri).first()
                else:
                    existing_membership = session.query(OireachtasMemberMembership).filter_by(
                        pId=pId,
                        houseCode=house.get("houseCode"),
                        houseNo=house.get("houseNo"),
                        membership_start_date=membership_start,
                    ).first()

                if existing_membership:
                    membership_obj = existing_membership
                else:
                    membership_obj = OireachtasMemberMembership(
                        pId=pId,
                        houseCode=house.get("houseCode"),
                        houseNo=house.get("houseNo"),
                        membership_start_date=membership_start,
                        membership_end_date=membership_end,
                        membership_uri=membership_uri,
                    )
                    session.add(membership_obj)
                    session.flush()

                # Process party memberships inside this membership
                party_wrappers = membership.get("parties", [])

                for wrapper in party_wrappers:
                    party = wrapper.get("party", {})
                    date_range = party.get("dateRange", {})

                    party_code = party.get("partyCode")
                    party_show = party.get("showAs")
                    party_start = self._parse_date(date_range.get("start"))
                    party_end = self._parse_date(date_range.get("end"))

                    # Avoid duplicates: check by membership_id & partyCode & dates
                    exists = session.query(OireachtasMemberPartyMembership).filter_by(
                        membership_id=membership_obj.membership_id,
                        partyCode=party_code,
                        party_start_date=party_start,
                    ).first()
                    if exists:
                        continue

                    pm_obj = OireachtasMemberPartyMembership(
                        membership_id=membership_obj.membership_id,
                        partyCode=party_code,
                        party_showAs=party_show,
                        party_start_date=party_start,
                        party_end_date=party_end,
                    )
                    session.add(pm_obj)
                    session.flush()

                # Process representations (constituencies)
                rep_wrappers = membership.get("represents", [])

                for wrapper in rep_wrappers:
                    rep = wrapper.get("represent", {})

                    represent_code = rep.get("representCode")
                    represent_show = rep.get("showAs")
                    represent_type = rep.get("representType")

                    exists = session.query(OireachtasMemberRepresentation).filter_by(
                        membership_id=membership_obj.membership_id,
                        representCode=represent_code,
                    ).first()
                    if exists:
                        continue

                    rep_obj = OireachtasMemberRepresentation(
                        membership_id=membership_obj.membership_id,
                        representCode=represent_code,
                        representShowAs=represent_show,
                        representType=represent_type,
                    )
                    session.add(rep_obj)
                    session.flush()

        # Commit at end of chunk
        session.commit()

    def _member_pid(self, node):
        # pId may be present as int or string in various fields
        if node is None:
            return None
        pid = node.get("pId") or node.get("pid") or node.get("id")
        return pid.strip() if isinstance(pid, str) and pid.strip() else None

