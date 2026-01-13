#experiments/factory.py
from logs.logger import get_logger

class ExperimentFactory:
    """
    Factory for managing experiment classes.
    """
    _registry = {}
    logger = get_logger("ExperimentFactory")

    @classmethod
    def register_experiment(cls, key: str, experiment_cls):
        """
        Register an experiment class under a key (e.g., "classification").
        """
        cls._registry[key] = experiment_cls
        cls.logger.info(f"Registered experiment: {key}")

    @classmethod
    def get_experiment(cls, key: str, **kwargs):
        """
        Instantiate an experiment from the registry with kwargs.
        """
        experiment_cls = cls._registry.get(key)
        cls.logger.info(f"Retrieving experiment class for key: {key}")
        if not experiment_cls:
            cls.logger.warning(f"Experiment '{key}' not found in registry")
            return None
        cls.logger.info(f"Instantiating experiment '{key}' with kwargs: {kwargs}")
        return experiment_cls(**kwargs)
