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
from data.models.boards_combined import BoardsConsolidated
from data.models.oireachtas_question_conceptual_model import OireachtasQuestionConceptualModel
from data.models.boards_conceptual_model import BoardsConceptualModel
from data.models.llm_topics import LLMTopic
from data.models.llm_topic_themes import LLMTopicThemes
from data.models.oireachtas_topic_summary import OireachtasTopicSummary
from data.models.llm_topic_alignment import LLMTopicAlignment

MODEL_REGISTRY = {
    "BoardsComment": BoardsComment,
    "BoardsDiscussion": BoardsDiscussion,
    "BoardsConsolidated": BoardsConsolidated,
    "OireachtasQuestion": OireachtasQuestion,
    "OireachtasParty": OireachtasParty,
    "OireachtasMember": OireachtasMember,
    "OireachtasMemberMembership": OireachtasMemberMembership,
    "OireachtasMemberPartyMembership": OireachtasMemberPartyMembership,
    "OireachtasMemberRepresentation": OireachtasMemberRepresentation,
    "OireachtasQuestionConceptualModel": OireachtasQuestionConceptualModel,
    "RobloxGame": RobloxGame,
    "SteamGame": SteamGame,
    "BoardsConceptualModel": BoardsConceptualModel,
    "LLMTopic": LLMTopic,
    "LLMTopicThemes": LLMTopicThemes,
    "OireachtasTopicSummary": OireachtasTopicSummary,
    "LLMTopicAlignment": LLMTopicAlignment,
}
