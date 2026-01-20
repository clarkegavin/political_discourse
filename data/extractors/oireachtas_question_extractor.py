#data/extractors/oireachtas_question_extractor.py
from data.abstract_connector import DBConnector
from sqlalchemy.orm import Session
from logs.logger import get_logger
from ..models import OireachtasQuestion
from ..parsers.oireachtas_answer_xml_parser import OireachtasAnswerXMLParser
from typing import List, Dict, Any


class OireachtasQuestionExtractor:
    def __init__(self, connector: DBConnector, chunk_size: int = 50):
        self._connector = connector
        self._chunk_size = chunk_size
        self.answer_parser = OireachtasAnswerXMLParser()
        self.logger = get_logger(self.__class__.__name__)

    def save_chunk(self, session: Session, records: List[Dict[str, Any]]):

        uris = [
            r.get("question", r).get("uri")
            for r in records
            if r.get("question", r).get("uri")
        ]

        existing = {
            uri for (uri,) in
            session.query(OireachtasQuestion.QuestionURI)
            .filter(OireachtasQuestion.QuestionURI.in_(uris))
            .all()
        }

        objs = []
        for r in records:
            q = r.get("question", r)  # fallback to entire dict

            uri = q.get("uri")

            if uri in existing:
                continue  # skip duplicates

            house = q.get("house", {})
            debate = q.get("debateSection", {})
            xml_uri = debate.get("formats", {}).get("xml", {}).get("uri")

            answer_xml = None
            answer_text = None
            speaker = None
            recorded_time = None

            if xml_uri:
                xml = self.answer_parser.fetch_xml(xml_uri)
                if xml:
                    parsed = self.answer_parser.parse(xml)
                    answer_xml = xml
                    answer_text = parsed.get("text")
                    speaker = parsed.get("speaker")
                    recorded_time = parsed.get("recorded_time")

            obj = OireachtasQuestion(
                QuestionURI=q.get("uri"),
                QuestionNumber=q.get("questionNumber"),
                QuestionType=q.get("questionType"),
                QuestionDate=q.get("date"),
                QuestionText=q.get("showAs") or "",
                AskedBy=q.get("by", {}).get("showAs"),
                AskedByURI=q.get("by", {}).get("uri"),
                AskedByMemberCode=q.get("by", {}).get("memberCode"),
                ToMinister=q.get("to", {}).get("showAs"),
                ToMinisterURI=q.get("to", {}).get("uri"),
                ToRoleCode=q.get("to", {}).get("roleCode"),
                ToRoleType=q.get("to", {}).get("roleType"),
                # AnswerText=q.get("answerText"),
                # AnswerDate=None,  # parse if API provides
                House=house.get("showAs"),
                ChamberType=house.get("chamberType"),
                CommitteeCode=house.get("committeeCode"),
                DebateSectionURI=debate.get("uri"),
                DebateSectionShowAs=debate.get("showAs"),
                AnswerXMLURI=xml_uri,
                AnswerXML=answer_xml,
                AnswerText=answer_text,
                AnswerSpeaker=speaker,
                AnswerRecordedTime=recorded_time,

                RawJSON=r,
            )
            objs.append(obj)

        if objs:
            session.bulk_save_objects(objs)
            session.commit()
            self.logger.info(f"Saved {len(records)} rows to OireachtasQuestion table")
        else:
            self.logger.info("No new records to save in this chunk.")

    def save_data(self, rows: list[dict]):
        """Save API-fetched data in chunks.  Checks for existing QuestionURI to avoid duplicates."""

        with self._connector.get_session() as session:
            for i in range(0, len(rows), self._chunk_size):
                chunk = rows[i:i + self._chunk_size]
                self.save_chunk(session, chunk)
        # with self._connector.get_session() as session:
        #     # Step 1: fetch all existing QuestionURIs once
        #     existing_uris = {
        #         row[0] for row in session.query(OireachtasQuestion.QuestionURI).all()
        #     }
        #
        #     for i in range(0, len(rows), self._chunk_size):
        #         chunk = rows[i:i + self._chunk_size]
        #
        #         # Step 2: filter out records that already exist
        #         chunk_to_insert = [r for r in chunk if r["QuestionURI"] not in existing_uris]
        #
        #         if not chunk_to_insert:
        #             continue  # nothing new in this chunk, skip
        #
        #         # Step 3: save the filtered chunk
        #         self.save_chunk(session, chunk_to_insert)
