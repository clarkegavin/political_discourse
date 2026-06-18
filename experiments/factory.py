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
        if key in cls._registry:
            return
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
        #cls.logger.info(f"Instantiating experiment '{key}' with kwargs: {kwargs}")
        return experiment_cls(**kwargs)

    @classmethod
    def build_experiment_from_config(cls, key: str, config: dict, X=None, global_config: dict = None):
        """Build an experiment instance from a configuration dictionary.

        The config may include top-level fields (name, model_name, evaluator_name, visualisations, etc.)
        and a `params` dict. This method merges them (params take precedence) and passes the combined
        kwargs to the experiment constructor. If X is provided and not present in params, it will be added.
        """
        experiment_cls = cls._registry.get(key)
        cls.logger.info(f"Building experiment from config for key: {key}")
        if not experiment_cls:
            cls.logger.warning(f"Experiment '{key}' not found in registry for build_from_config")
            return None

        # Start with params copy
        params = dict(config.get("params", {}) or {})

        # Merge top-level keys into params (do not override existing params keys)
        for k, v in config.items():
            if k in ("params", "sweep", "overrides", "run_name"):
                continue
            # do not override keys already in params
            if k not in params:
                params[k] = v

        # Attach X if provided and not already present
        if X is not None and "X" not in params:
            params["X"] = X

        # Attach global_config if not present
        if global_config is not None and "global_config" not in params:
            params["global_config"] = global_config

        # Instantiate
        try:
            cls.logger.debug(f"Instantiating experiment class '{key}' with params keys: {list(params.keys())} and sample values: { {k: params[k] for k in list(params.keys())[:5]} }")
            return experiment_cls(**params)
        except Exception as e:
            cls.logger.error(f"Failed to build experiment '{key}' from config: {e}")
            raise
