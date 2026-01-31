# data/extractors/boards_discussion_extractor.py
from dateutil.parser import isoparse
from data.models.boards_discussion import BoardsDiscussion

class BoardsDiscussionExtractor:

    def __init__(self, connector, chunk_size=500):
        self.connector = connector
        self.chunk_size = chunk_size
        self.logger = connector.logger

    def save_data(self, records: list[dict]):
        """Save all records as a snapshot using bulk insert."""
        session = self.connector.get_session()

        try:
            objs = []
            for r in records:
                objs.append(
                    BoardsDiscussion(
                        DiscussionId=str(r.get("discussionID")),
                        Type=r.get("type"),
                        Title=r.get("name"),
                        Body=r.get("body"),
                        CategoryId=r.get("categoryID"),
                        DateInserted=self._dt(r.get("dateInserted")),
                        DateUpdated=self._dt(r.get("dateUpdated")),
                        DateLastComment=self._dt(r.get("dateLastComment")),
                        insertUserId=r.get("insertUserID"),
                        UpdateUserId=r.get("updateUserID"),
                        LastUserId=r.get("lastUserID"),
                        Closed=str(r.get("closed")),
                        countComments=r.get("countComments"),
                        CanonicalURL=r.get("canonicalUrl"),
                    )
                )

            if objs:
                session.bulk_save_objects(objs)
                session.commit()
                self.logger.info(f"Saved {len(objs)} discussions as snapshot.")
            else:
                self.logger.info("No discussions to save.")

        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @staticmethod
    def _dt(value):
        return isoparse(value) if value else None
