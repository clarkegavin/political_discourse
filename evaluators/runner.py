# evaluators/runner.py
from typing import Dict, Any
from logs.logger import get_logger
from evaluators.factory import EvaluatorFactory

logger = get_logger("EvaluationRunner")


class EvaluationRunner:
    """Responsible for invoking evaluators on experiment results and returning metrics/artifacts.

    Minimal implementation for Phase 1; this does not touch MLflow.
    """

    def __init__(self):
        self.logger = logger

    def evaluate(self, result: Dict[str, Any], evaluator_cfg: Dict[str, Any]):
        """Run the configured evaluator against the experiment result.

        result: dict returned by Experiment.run() expected to have keys 'df' and 'metadata'
        evaluator_cfg: either a string name or dict with 'name' and 'params'
        """
        if isinstance(evaluator_cfg, str):
            name = evaluator_cfg
            params = {}
        elif isinstance(evaluator_cfg, dict):
            name = evaluator_cfg.get("name")
            params = evaluator_cfg.get("params", {})
        else:
            raise ValueError("Invalid evaluator_cfg")


        self.logger.info(f"Running evaluator '{name}'")

        metadata = result.get("metadata", {})
        params = {
            **params,
            **metadata.get("evaluator_params", {})
            #"combined_text_field_name": metadata.get("combined_text_field_name")
        }

        evaluator = EvaluatorFactory.get_evaluator(name, **params)
        if not evaluator:
            self.logger.warning(f"EvaluatorFactory returned None for '{name}'")
            return {}

        try:
            #metrics = evaluator.evaluate(result.get("df"), result.get("metadata", {}), evaluator_cfg.get("params", {}))
            metadata = result.get("metadata", {})
            self.logger.info(f"Evaluator params being passed: {params}")
            self.logger.info(f"Result keys: {list(result.keys())}")
            self.logger.info(f"Metadata keys: {list(result.get('metadata', {}).keys())}")

            metrics = evaluator.evaluate(
                result.get("df"),
                metadata.get("topics"),
                metadata.get("model")
            )
            return {"metrics": metrics, "artifacts": []}
        except Exception as e:
            self.logger.error(f"Evaluator '{name}' failed: {e}")
            return {"metrics": {}, "artifacts": []}

