#data/data_source/factory.py
from .db_discussion_source import DBDiscussionSource
from .api_discussion_source import APIDiscussionSource


class DiscussionSourceFactory:

    @staticmethod
    def create(
        cfg: dict,
        *,
        connector=None,
        fetcher=None,
    ):
        source_type = cfg.get("type")

        if source_type == "db":
            return DBDiscussionSource(
                connector=connector,
                table=cfg.get("table", "dbo.boards_discussions"),
                where=cfg.get("where"),
            )

        if source_type == "api":
            return APIDiscussionSource(
                fetcher=fetcher,
                category_id=cfg["category_id"],
                limit=cfg.get("limit", 100),
            )

        raise ValueError(f"Unknown discussion source type: {source_type}")
