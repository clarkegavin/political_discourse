# experiments/base.py
from abc import ABC, abstractmethod
import mlflow
from logs.logger import get_logger
from typing import Optional, Union

class Experiment(ABC):
    """
    Abstract base class for all experiments.
    Supports optional MLflow lifecycle management.
    """

    def __init__(self, name: str, mlflow_tracking: bool = True, mlflow_experiment: Optional[str] = None):
        self.name = name
        self.mlflow_tracking = mlflow_tracking
        self.mlflow_experiment = mlflow_experiment
        self.logger = get_logger(self.__class__.__name__)
        self._mlflow_active = False

    def __enter__(self):
        """Start MLflow run if tracking is enabled."""
        if self.mlflow_tracking:
            mlflow.set_experiment(self.mlflow_experiment or "default_experiment")
            mlflow.start_run(run_name=self.name, nested=False)
            self._mlflow_active = True
            self.logger.info(f"Started MLflow run for experiment: {self.name}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensure MLflow run ends cleanly, even on error."""
        if self._mlflow_active:
            if exc_type:
                self.logger.error(f"Experiment '{self.name}' failed: {exc_val}")
            mlflow.end_run()
            self.logger.info(f"Ended MLflow run for experiment: {self.name}")
            self._mlflow_active = False

    @abstractmethod
    def run(self, *args, **kwargs):
        pass

