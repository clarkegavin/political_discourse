#data/models/registry.py
from data.models.boards_comment import BoardsComment
from data.models.boards_discussion import BoardsDiscussion
from data.models.roblox import RobloxGame
from data.models.steam import SteamGame
from data.models.oireachtas_question import OireachtasQuestion
from data.models.oireachtas_party import OireachtasParty

MODEL_REGISTRY = {
    "BoardsComment": BoardsComment,
    "BoardsDiscussion": BoardsDiscussion,
    "OireachtasQuestion": OireachtasQuestion,
    "OireachtasParty": OireachtasParty,
    "RobloxGame": RobloxGame,
    "SteamGame": SteamGame,
}
