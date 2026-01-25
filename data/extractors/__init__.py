#data/extractors/__init__.py
from .oireachtas_debate_extractor import OireachtasDebateExtractor
from .oireachtas_question_extractor import OireachtasQuestionExtractor
__all__ = [
    "OireachtasDebateExtractor",
    "OireachtasQuestionExtractor",
]