# experiments/base.py
from abc import ABC, abstractmethod
from logs.logger import get_logger
from typing import Optional, Union

class Experiment(ABC):
    """
    Abstract base class for all experiments.
    Experiment should be a pure algorithmic unit: it must NOT manage MLflow lifecycle.
    """

    def __init__(self, name: str, mlflow_tracking: bool = True, mlflow_experiment: Optional[str] = None):
        self.name = name
        self.mlflow_tracking = mlflow_tracking
        self.mlflow_experiment = mlflow_experiment
        self.logger = get_logger(self.__class__.__name__)

    def __enter__(self):
        """No-op: MLflow lifecycle is managed by ExperimentRunner."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """No-op: MLflow lifecycle is managed by ExperimentRunner."""
        return False

    @abstractmethod
    def run(self, *args, **kwargs):
        pass

    def collect_params(self) -> dict:
        """Return a dict of parameters the experiment was created with for external logging.
        Override in subclasses to expose model/evaluator/preprocessing params."""
        return {}
