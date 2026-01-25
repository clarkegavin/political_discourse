# data/extractors/oireachtas_debate_extractor.py
from typing import List, Dict
from sqlalchemy.orm import Session
from logs.logger import get_logger
from ..models import (
    OireachtasDebateSection,
    OireachtasDebateContribution,
)


class OireachtasDebateExtractor:
    def __init__(self, connector, chunk_size: int = 100):
        self.connector = connector
        self.chunk_size = chunk_size
        self.logger = get_logger(self.__class__.__name__)

    def save_data(self, debates: List[Dict]):
        self.logger.info(f"Saving {len(debates)} debate records to the database")
        with self.connector.get_session() as session:
            for debate in debates:
                self._process_debate(session, debate)
            session.commit()

    def _process_debate(self, session: Session, debate: Dict):
        debate_record = debate_wrapper.get("debateRecord")
        if not debate_record:
            return

        debate_date = debate_record.get("date")

        sections = debate_record.get("debateSections", [])

        for section_wrapper in sections:
            section = section_wrapper.get("debateSection")
            if not section:
                continue

            section_id = section.get("debateSectionId")
            if not section_id:
                continue

            debate_section = OireachtasDebateSection(
                DebateSectionId=section_id,
                DebateDate=debate_date,
                Title=section.get("showAs"),
                RawJSON=section,
            )

            session.add(debate_section)

            # need to rewrite this as contributions doesn't always exist
            contributions = section.get("contributions", [])
            for idx, c in enumerate(contributions):
                text = c.get("text")
                if not text:
                    continue

                contribution = OireachtasDebateContribution(
                    DebateSectionId=section_id,
                    SpeakerName=c.get("speaker", {}).get("showAs"),
                    SpeakerURI=c.get("speaker", {}).get("uri"),
                    SpeakerRole=c.get("speaker", {}).get("role"),
                    ContributionType=c.get("contributionType"),
                    ContributionText=text,
                    Sequence=idx,
                    RawJSON=c,
                )

                session.add(contribution)
