# data/extractors/boards_comments_extractor.py
from dateutil.parser import isoparse
from data.models.boards_comment import BoardsComment

class BoardsCommentsExtractor:

    def __init__(self, connector, chunk_size=500):
        self.connector = connector
        self.chunk_size = chunk_size
        self.logger = connector.logger

    def save_data(self, records: list[dict]):
        """Save all comment records using bulk insert."""
        session = self.connector.get_session()

        try:
            objs = []
            for r in records:
                objs.append(
                    BoardsComment(
                        commentID=r.get("commentID"),
                        discussionID=r.get("discussionID"),
                        parentRecordType=r.get("parentRecordType"),
                        parentRecordID=r.get("parentRecordID"),
                        name=r.get("name"),
                        categoryID=r.get("categoryID"),
                        body=r.get("body"),
                        dateInserted=self._dt(r.get("dateInserted")),
                        dateUpdated=self._dt(r.get("dateUpdated")),
                        updateUserID=r.get("updateUserID"),
                        score=r.get("score"),
                        depth=r.get("depth"),
                        scoreChildComments=r.get("scoreChildComments"),
                        countChildComments=r.get("countChildComments"),
                        url=r.get("url"),
                        type=r.get("type"),
                        format=r.get("format"),
                        attributes=r.get("attributes"),
                    )
                )

            if objs:
                session.bulk_save_objects(objs)
                session.commit()
                self.logger.info(f"Saved {len(objs)} comments to DB.")
            else:
                self.logger.info("No comments to save.")

        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @staticmethod
    def _dt(value):
        return isoparse(value) if value else None

