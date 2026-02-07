#fetcher/__init__.py
from .base import Fetcher
from .factory import FetcherFactory
from .steam_app_list_fetcher import SteamAppListFetcher
from .steam_store_fetcher import SteamStoreFetcher
from .steam_review_fetcher import SteamReviewFetcher
from .steam_current_user_fetcher import SteamCurrentUserFetcher
from .oireachtas_question_fetcher import OireachtasQuestionFetcher
from .oireachtas_debate_fetcher import OireachtasDebateFetcher
from .oireachtas_party_fetcher import OireachtasPartyFetcher
from .oireachtas_members_fetcher import OireachtasMembersFetcher
from .boards_fetcher import BoardsFetcher
from .boards_comments_fetcher import BoardsCommentsFetcher


# Register the SteamAppListFetcher with the factory
FetcherFactory.register("steam_app_list", SteamAppListFetcher)
# Register the SteamStoreFetcher with the factory
FetcherFactory.register("steam_store", SteamStoreFetcher)
# Register the SteamReviewFetcher with the factory
FetcherFactory.register("steam_review", SteamReviewFetcher)
# Register other fetchers as needed
FetcherFactory.register("steam_current_user", SteamCurrentUserFetcher)
# Register the OireachtasQuestionFetcher with the factory
FetcherFactory.register("oireachtas_questions", OireachtasQuestionFetcher)
# Register the OireachtasDebateFetcher with the factory
FetcherFactory.register("oireachtas_debates", OireachtasDebateFetcher)
# Register the OireachtasPartyFetcher with the factory
FetcherFactory.register("oireachtas_parties", OireachtasPartyFetcher)
# Register the Oireachtas Members fetcher
FetcherFactory.register("oireachtas_members", OireachtasMembersFetcher)
# Register the Boards fetcher
FetcherFactory.register("boards", BoardsFetcher)
# Register the Boards comments fetcher
FetcherFactory.register("boards_comments", BoardsCommentsFetcher)


__all__ = [
    "Fetcher",
    "FetcherFactory",
    "SteamAppListFetcher",
    "SteamStoreFetcher",
    "SteamReviewFetcher",
    "SteamCurrentUserFetcher",
    "OireachtasQuestionFetcher",
    "OireachtasPartyFetcher",
    "OireachtasMembersFetcher",
    "BoardsFetcher",
    "BoardsCommentsFetcher",
]
