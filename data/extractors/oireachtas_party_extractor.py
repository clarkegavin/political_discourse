# data/extractors/oireachtas_party_extractor.py
from typing import List, Dict
from data.models.oireachtas_party import OireachtasParty


class OireachtasPartyExtractor:
    def __init__(self, connector, chunk_size: int = 500):
        self.connector = connector
        self.chunk_size = chunk_size
        self.logger = connector.logger

    def save_data(self, records: List[Dict]):
        """Save records in bulk to dbo.oireachtas_parties."""
        session = self.connector.get_session()
        try:
            objs = []
            for r in records:
                # Support both mapped records (from pipeline) and raw API records
                if "party_show_as" in r:
                    party_show = r.get("party_show_as")
                    party_code = r.get("party_code")
                    house_code = r.get("house_code")
                    house_no = r.get("house_no")
                else:
                    party = r.get("party") or r
                    house = r.get("house") or party.get("house") or {}
                    party_show = party.get("showAs")
                    party_code = party.get("partyCode")
                    house_code = house.get("houseCode")
                    house_no = house.get("houseNo")

                objs.append(
                    OireachtasParty(
                        PartyShowAs=party_show,
                        PartyCode=party_code,
                        HouseCode=house_code,
                        HouseNo=house_no,
                    )
                )

            if objs:
                session.bulk_save_objects(objs)
                session.commit()
                self.logger.info(f"Saved {len(objs)} parties to OireachtasParty table")
            else:
                self.logger.info("No party records to save.")
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
