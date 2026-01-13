# pipelines/experiment_pipeline.py
from typing import Dict, Any, Optional, List
from .base import Pipeline
from logs.logger import get_logger
from experiments.factory import ExperimentFactory
from preprocessing.factory import PreprocessorFactory
from preprocessing.sequential import SequentialPreprocessor

class ExperimentPipeline(Pipeline):
    """
    Pipeline that runs one or more experiments for a given model type.
    """
    def __init__(
        self,
        experiment_type: str,
        model_name: str,
        evaluator_name: str,
        metrics: List[str],
        experiments: Optional[List[Dict[str, Any]]] = None,
        mlflow_experiment: Optional[str] = None,
        name: Optional[str] = None,
        global_config: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        super().__init__(name=name or model_name)
        self.experiment_type = experiment_type
        self.model_name = model_name
        self.evaluator_name = evaluator_name
        self.metrics = metrics
        self.experiments = experiments or [{}]
        self.mlflow_experiment = mlflow_experiment
        self.global_config = global_config or {}
        self.logger = get_logger(self.__class__.__name__)

    @classmethod
    def from_config(cls, entry: Dict[str, Any], global_config=None) -> "ExperimentPipeline":
        params = entry.get("params", {})
        return cls(**params, name=entry.get("name"), global_config=global_config or {})

    def execute(self, X_train, X_test, y_train, y_test, target_encoder=None):
        self.logger.info(f"Running experiments for model '{self.model_name}'")
        mlflow_experiment_name = self.mlflow_experiment or f"{self.model_name}_experiments"
        self.logger.info(f"Target encoder provided: {target_encoder is not None}")

        for i, exp_cfg in enumerate(self.experiments, start=1):
            run_name = exp_cfg.get("run_name", f"{self.model_name}_run{i}")
            self.logger.info(f"Starting experiment {i} ({run_name}) with params {exp_cfg.get('params', {})}")

            X_train_exp = X_train.copy()
            X_test_exp = X_test.copy()

            self.logger.info("Applying experiment-specific preprocessing")
            X_train_exp, X_test_exp, preprocessing_metadata  = self._preprocessing(exp_cfg, X_train_exp, X_test_exp)
            self.logger.info("Experiment-specific preprocessing complete")
            exp_params = {
                "name": run_name,
                "model_name": self.model_name,
                "evaluator_name": self.evaluator_name,
                "metrics": self.metrics,
                "mlflow_experiment": mlflow_experiment_name,
                "target_encoder": target_encoder,
                "preprocessing_metadata": preprocessing_metadata,
                **exp_cfg.get("params", {})
            }

            self.logger.info(f'Experiment parameters: {exp_params}')
            experiment = ExperimentFactory.get_experiment(self.experiment_type, **exp_params)
            self.logger.info(f"Created experiment instance: {experiment}")
            if experiment:
                self.logger.info(f"Executing experiment '{run_name}'")
                experiment.run(X_train_exp, X_test_exp, y_train, y_test)

        self.logger.info(f"All experiments for '{self.model_name}' complete.")


    def _preprocessing(self, exp_cfg, X_train, X_test):
        """
        Applies only experiment preprocessing, returns updated data and full metadata.
        """
        preprocessing_metadata, exp_pre_cfgs = self._get_full_metadata(exp_cfg)

        if exp_pre_cfgs:
            steps = [PreprocessorFactory.create(pre["name"], **pre.get("params", {}))
                     for pre in exp_pre_cfgs]

            self.logger.info(f"Applying per-experiment preprocessing steps: {steps}")
            preprocessor = SequentialPreprocessor(steps)
            self.logger.info(f"Preprocessing steps created: {preprocessor}")

            text_field = exp_cfg.get("text_field")
            self.logger.info(f"Preprocessing text field: {text_field}")
            X_train[text_field] = preprocessor.fit_transform(X_train[text_field])
            X_test[text_field] = preprocessor.transform(X_test[text_field])
            self.logger.info("Preprocessing applied to training and testing data.")
        self.logger.info(f"Full preprocessing metadata: {preprocessing_metadata}")
        return X_train, X_test, preprocessing_metadata

    def _get_full_metadata(self, exp_cfg):
        """
        Extract metadata for the experiment:
        - Global preprocessing (log only)
        - Experiment preprocessing (log + apply)
        - Roblox extraction params
        - Data split params
        """
        metadata = {
            "global_preprocessing": [],
            "experiment_preprocessing": [],
            "roblox_extraction": {},
            "split_data": {}
        }

        # --- Global preprocessing ---
        for p in self.global_config.get("pipelines", []):
            if p.get("name") == "preprocess_text":
                global_pre_cfgs = p.get("params", {}).get("preprocessors", [])
                for pre_cfg in global_pre_cfgs:
                    metadata["global_preprocessing"].append({
                        "name": pre_cfg["name"],
                        "params": pre_cfg.get("params", {})
                    })
                break

        # --- Experiment preprocessing ---
        exp_pre_cfgs = exp_cfg.get("preprocessing", [])
        for pre_cfg in exp_pre_cfgs:
            metadata["experiment_preprocessing"].append({
                "name": pre_cfg["name"],
                "params": pre_cfg.get("params", {})
            })

        # --- Roblox extraction ---
        for p in self.global_config.get("pipelines", []):
            if p.get("name") == "roblox_extraction":
                metadata["roblox_extraction"] = p.get("params", {})
                break

        # --- Data splitting ---
        for p in self.global_config.get("pipelines", []):
            if p.get("name") == "split_data":
                metadata["split_data"] = p.get("params", {})
                break

        return metadata, exp_pre_cfgs
