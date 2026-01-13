#evaluators/factory.py
from logs.logger import get_logger

class EvaluatorFactory:
    """
    Factory for managing evaluators.
    """
    _registry = {}

    logger = get_logger("EvaluatorFactory")

    @classmethod
    def register_evaluator(cls, name: str, evaluator):
        cls._registry[name] = evaluator
        cls.logger.info(f"Registered evaluator: {name}")

    @classmethod
    def get_evaluator(cls, name: str = None, **kwargs):
        cls.logger.info(f"Retrieving evaluator: {name} with params {kwargs}")
        evaluator = cls._registry.get(name)
        if not evaluator:
            cls.logger.warning(f"Evaluator '{name}' not found in registry")
        cls.logger.info(f"Evaluator found: {evaluator}")
        return evaluator(name, **kwargs) if evaluator else None