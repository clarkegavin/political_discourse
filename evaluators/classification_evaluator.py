#evaluators/classification_evaluator.py
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from .base import Evaluator
from .factory import EvaluatorFactory
from logs.logger import get_logger

class ClassificationEvaluator(Evaluator):
    """
    Evaluator for classification models.
    """

    logger = get_logger("ClassificationEvaluator")

    def __init__(self, name: str, metrics: list = None, **kwargs):
        self.logger.info(f"Initializing {name}")
        super().__init__(name, **kwargs)
        self.name = name
        self.metrics = metrics if metrics is not None else ['accuracy', 'precision', 'recall', 'f1_score']

    def evaluate(self, y_true, y_pred, prefix: str ='') -> dict:
        """
        Evaluate classification model performance.
        """
        self.logger.info(f"Evaluating model with {self.name}")
        metrics = {}
        for metric in self.metrics:
            if metric == 'accuracy':
                metrics[f'{prefix}accuracy'] = accuracy_score(y_true, y_pred)
            elif metric == 'precision':
                metrics[f'{prefix}precision'] = precision_score(
                    y_true, y_pred, average='weighted', zero_division=0)
            elif metric == 'recall':
                metrics[f'{prefix}recall'] = recall_score(
                    y_true, y_pred, average='weighted', zero_division=0)
            elif metric == 'f1_score':
                metrics[f'{prefix}f1_score'] = f1_score(
                    y_true, y_pred, average='weighted', zero_division=0)
            else:
                self.logger.warning(f"Metric '{metric}' is not supported.")

        self.logger.info(f'Evaluation metrics: {metrics}')
        return metrics

        # Optional override: CV eval
    def evaluate_cv(self, cv_scores: dict, prefix: str = '') -> dict:
        """
        Averages CV metrics if needed.
        """
        self.logger.info(f"Evaluating CV scores for {self.name}")
        return {f"{prefix}{k}": float(v) for k, v in cv_scores.items()}