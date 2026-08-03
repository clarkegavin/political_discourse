# pipelines/topic_modeling_pipeline.py


from typing import Dict, Any, Optional, List
from .base import Pipeline
from config.config_loader import ConfigLoader
from preprocessing.factory import PreprocessorFactory
from preprocessing.sequential import SequentialPreprocessor
from logs.logger import get_logger

class TopicModelingPipeline(Pipeline):
    """Pipeline to run topic modelling experiments on a full dataset.

    Differences from supervised ExperimentPipeline:
    - operates on a single dataset `X` (no train/test split)
    - accepts `text_field` (str or list) which is combined to form the documents
    - preserves all other metadata columns and attaches topic assignments back
    - supports multiple experiments and per-experiment preprocessing that only
      modifies the text column
    """

    def __init__(
        self,
        model_name: str,
        evaluator_name: str,
        dataset_name: Optional[str] = None,
        experiments: Optional[List[Dict[str, Any]]] = None,
        experiment_refs: Optional[List[str]] = None,
        mlflow_experiment: Optional[str] = None,
        name: Optional[str] = None,
        global_config: Optional[Dict[str, Any]] = None,
        default_text_field: Optional[Any] = "text",
        combined_text_field_name: str = "__topic_input_text__",
        **kwargs,
    ) -> None:
        super().__init__(name=name or "TopicModelingPipeline")
        self.model_name = model_name
        self.dataset_name = dataset_name
        self.evaluator_name = evaluator_name
        self.experiments = experiments or [{}]
        # experiment_refs: list of YAML file paths to load experiment configs from
        self.experiment_refs = experiment_refs or []
        self.mlflow_experiment = mlflow_experiment
        self.global_config = global_config or {}
        self.logger = get_logger(self.__class__.__name__)
        self.default_text_field = default_text_field
        self.combined_text_field_name = combined_text_field_name
        self._cfg_loader = ConfigLoader()
        self.logger.info(f"Initialized TopicModelingPipeline with model '{self.model_name}', evaluator '{self.evaluator_name}', dataset '{self.dataset_name}'")

    @classmethod
    def from_config(cls, entry: Dict[str, Any], global_config=None) -> "TopicModelingPipeline":
        # Prefer explicit experiment_refs over inline experiments.
        params = entry.get("params", {}) or {}
        # experiment_refs may be provided either at top-level or inside params
        experiment_refs = params.pop("experiment_refs", None)
        if experiment_refs is None:
            experiment_refs = entry.get("experiment_refs", None)

        # Ensure we do not rely on inline experiments for topic modelling; pass empty experiments
        return cls(
            **{k: v for k, v in params.items() if k != "experiments"},
            experiments=[],
            experiment_refs=experiment_refs,
            name=entry.get("name"),
            global_config=global_config or {},
        )

    def execute(self, data=None):
        """Run configured topic modelling experiments on full dataset X.

        Parameters
        - data: pandas DataFrame containing metadata + text columns
        """
        X = data
        self.logger.info("Starting topic modelling pipeline")
        tf = self.default_text_field

        # Load experiment configs: ONLY load from experiment_refs; do NOT fall back to inline experiments.
        experiment_configs: List[Dict[str, Any]] = []
        if not getattr(self, "experiment_refs", None):
            self.logger.error("No 'experiment_refs' provided for TopicModelingPipeline; cannot run experiments")
            return X

        for ref in self.experiment_refs:
            try:
                loaded = self._cfg_loader.load_file(ref)
                # normalize: if file contains 'experiments' top-level key, extend; else append
                if isinstance(loaded, dict) and "experiments" in loaded:
                    experiment_configs.extend(loaded.get("experiments", []))
                elif isinstance(loaded, list):
                    experiment_configs.extend(loaded)
                else:
                    experiment_configs.append(loaded)
            except Exception as e:
                self.logger.error(f"Failed to load experiment ref {ref}: {e}")

        data_with_topics = None

        # Build a list of runner-ready experiment configs, applying per-experiment preprocessing
        runner_experiments: List[Dict[str, Any]] = []
        for i, exp_cfg in enumerate(experiment_configs, start=1):
            run_name = exp_cfg.get("run_name", f"{self.model_name}_run{i}")
            self.logger.info(f"Preparing topic experiment {i} ({run_name})")

            # Work on a copy so metadata is preserved
            self.logger.info("Topic modelling pipeline - data type is %s", type(X))
            X_exp = X.copy()

            # Preprocessing for experiment (only applied to text field)
            preprocessing_steps = exp_cfg.get("preprocessing", [])
            if preprocessing_steps:
                steps = [PreprocessorFactory.create(pre["name"], **pre.get("params", {}))
                         for pre in preprocessing_steps]
                preprocessor = SequentialPreprocessor(steps)

                self.logger.info(f"Applying {len(steps)} preprocessors to DataFrame")
                X_exp = preprocessor.fit_transform(X_exp)

            # Combine text fields (after preprocessing if applied)
            if isinstance(tf, (list, tuple)):
                combined = X_exp[tf[0]].fillna("")
                for col in tf[1:]:
                    combined = combined + " " + X_exp[col].fillna("")
            else:
                combined = X_exp[tf].fillna("")

            X_exp[self.combined_text_field_name] = combined

            self.logger.info(f"Combined text fields into '{self.combined_text_field_name}' for experiment {run_name}")
            # Prepare a run-style experiment config expected by ExperimentRunner
            runner_exp = {
                "run_name": run_name,
                "sweep": exp_cfg.get("sweep"),
                "save_path": exp_cfg.get("save_path"),
                "params": {
                    **exp_cfg.get("params", {}),
                    "name": run_name,
                    "model_name": self.model_name,
                    "evaluator_name": self.evaluator_name,
                    "mlflow_experiment": self.mlflow_experiment,
                    "preprocessing_metadata": {"experiment_preprocessing": preprocessing_steps},
                    "X": X_exp,
                    "dataset_name": self.dataset_name,
                    "visualisations": exp_cfg.get("visualisations", []),
                    "combined_text_field_name": self.combined_text_field_name,
                },
            }
            self.logger.info(f"Runner experiment built with sweep: {exp_cfg.get('sweep') is not None}")
            runner_experiments.append(runner_exp)

        # Delegate entire list to attached ExperimentRunner
        runner = getattr(self, "experiment_runner", None)
        if runner is None:
            raise RuntimeError("ExperimentRunner not attached to pipeline")

        results = runner.run_experiments(
            experiment_type="topic_modeling",
            experiments=runner_experiments,
            global_config={"mlflow_experiment": self.mlflow_experiment},
        )

        # Use first run's df as return value if available
        if results and isinstance(results, list):
            first = results[0]
            if isinstance(first, dict):
                data_with_topics = first.get("result", {}).get("df")

        self.logger.info("-------------------------All experiments complete-------------------------")

        self.logger.info("Topic modelling pipeline complete.")

        if data_with_topics is not None:
            self.logger.info("Returning data with attached topic assignments")
            return data_with_topics
        else:
            self.logger.warning("No data to return from topic modelling pipeline")
            return X
