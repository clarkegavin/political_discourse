#data/models/__init__.py

from .base import Base

from .steam import SteamGame
from .roblox import RobloxGame

from .oireachtas_question import OireachtasQuestion
from .oireachtas_debate import OireachtasDebateSection
from .oireachtas_contribution import OireachtasDebateContribution
from .boards_comment import BoardsComment
from .boards_discussion import BoardsDiscussion


#register models for easy import


__all__ = [
    "Base",
    "SteamGame",
    "RobloxGame",
    "OireachtasQuestion",
    "OireachtasDebateSection",
    "OireachtasDebateContribution",
    "BoardsComment",
    "BoardsDiscussion",
]