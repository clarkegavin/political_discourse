#data/models/registry.py
from data.models.boards_comment import BoardsComment
from data.models.boards_discussion import BoardsDiscussion
from data.models.roblox import RobloxGame
from data.models.steam import SteamGame
from data.models.oireachtas_question import OireachtasQuestion
from data.models.oireachtas_party import OireachtasParty
from data.models.oireachtas_member import OireachtasMember
from data.models.oireachtas_member_membership import OireachtasMemberMembership
from data.models.oireachtas_member_party_membership import OireachtasMemberPartyMembership
from data.models.oireachtas_member_representation import OireachtasMemberRepresentation

MODEL_REGISTRY = {
    "BoardsComment": BoardsComment,
    "BoardsDiscussion": BoardsDiscussion,
    "OireachtasQuestion": OireachtasQuestion,
    "OireachtasParty": OireachtasParty,
    "OireachtasMember": OireachtasMember,
    "OireachtasMemberMembership": OireachtasMemberMembership,
    "OireachtasMemberPartyMembership": OireachtasMemberPartyMembership,
    "OireachtasMemberRepresentation": OireachtasMemberRepresentation,
    "RobloxGame": RobloxGame,
    "SteamGame": SteamGame,
}
