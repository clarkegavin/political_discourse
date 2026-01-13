#evaluators/base.py
from abc import ABC, abstractmethod
from logs.logger import get_logger

class Evaluator(ABC):
    """
    Abstract base class for model evaluators.
    """

    def __init__(self, name: str, **kwargs):
        self.name = name
        self.logger = get_logger(f"Evaluator:{name}")
        # store any additional common kwargs if needed
        for key, value in kwargs.items():
            setattr(self, key, value)
        self.logger.info(f"Initialized base evaluator '{name}' with kwargs: {kwargs}")


    @abstractmethod
    def evaluate(self, y_true, y_pred, prefix: str ='') -> dict:
        """
        Evaluate the model predictions against true values.
        """
        pass

    def evaluate_cv(self, cv_scores: dict, prefix: str = '') -> dict:
        """
        Optional: evaluation of cross-validation results.
        Default implementation logs and returns the scores unchanged.

        This method is NOT abstract, so it won't break existing evaluators.
        """
        self.logger.info(f"Evaluating cross-validation results for {self.name}")
        return {f"{prefix}{k}": v for k, v in cv_scores.items()}