 # pipelines/topic_modeling_pipeline.py


from typing import Dict, Any, Optional, List
from .base import Pipeline
from logs.logger import get_logger
from experiments.factory import ExperimentFactory
from preprocessing.factory import PreprocessorFactory
from preprocessing.sequential import SequentialPreprocessor

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
        experiments: Optional[List[Dict[str, Any]]] = None,
        mlflow_experiment: Optional[str] = None,
        name: Optional[str] = None,
        global_config: Optional[Dict[str, Any]] = None,
        default_text_field: Optional[Any] = "text",
        combined_text_field_name: str = "__topic_input_text__",
        **kwargs,
    ) -> None:
        super().__init__(name=name or "TopicModelingPipeline")
        self.model_name = model_name
        self.evaluator_name = evaluator_name
        self.experiments = experiments or [{}]
        self.mlflow_experiment = mlflow_experiment
        self.global_config = global_config or {}
        self.logger = get_logger(self.__class__.__name__)
        self.default_text_field = default_text_field
        self.combined_text_field_name = combined_text_field_name

    @classmethod
    def from_config(cls, entry: Dict[str, Any], global_config=None) -> "TopicModelingPipeline":
        params = entry.get("params", {})
        return cls(**params, name=entry.get("name"), global_config=global_config or {})

    def execute(self, data=None):
        """Run configured topic modelling experiments on full dataset X.

        Parameters
        - data: pandas DataFrame containing metadata + text columns
        """
        X = data
        self.logger.info("Starting topic modelling pipeline")
        tf = self.default_text_field

        for i, exp_cfg in enumerate(self.experiments, start=1):
            run_name = exp_cfg.get("run_name", f"{self.model_name}_run{i}")
            self.logger.info(f"Starting topic experiment {i} ({run_name})")

            # Work on a copy so metadata is preserved
            X_exp = X.copy()

            # Preprocessing for experiment (only applied to text field)
            preprocessing_steps = exp_cfg.get("preprocessing", [])
            if preprocessing_steps:
                steps = [PreprocessorFactory.create(pre["name"], **pre.get("params", {}))
                         for pre in preprocessing_steps]
                preprocessor = SequentialPreprocessor(steps)

                # Combine text fields if necessary
                if isinstance(tf, (list, tuple)):
                    combined = X_exp[tf[0]].fillna("")
                    for col in tf[1:]:
                        combined = combined + " " + X_exp[col].fillna("")
                else:
                    combined = X_exp[tf].fillna("")

                self.logger.info(f"Applying {len(steps)} preprocessors to text field(s): {tf}")
                combined_transformed = preprocessor.fit_transform(combined)

                # Attach back to a dedicated column used by the experiment
                X_exp[self.combined_text_field_name] = combined_transformed
            else:
                # No preprocessors: just combine fields into column
                if isinstance(tf, (list, tuple)):
                    combined = X_exp[tf[0]].fillna("")
                    for col in tf[1:]:
                        combined = combined + " " + X_exp[col].fillna("")
                else:
                    combined = X_exp[tf].fillna("")
                X_exp[self.combined_text_field_name] = combined

            # Prepare params for the experiment
            exp_params = {
                "name": run_name,
                "model_name": self.model_name,
                "evaluator_name": self.evaluator_name,
                "mlflow_experiment": self.mlflow_experiment,
                "preprocessing_metadata": {
                    "experiment_preprocessing": preprocessing_steps
                },
                "X": X_exp,
                "visualisations": exp_cfg.get("visualisations", []),
                **exp_cfg.get("params", {}),

            }

            self.logger.info(f"Instantiating topic modelling experiment with params: {exp_params.keys()}")
            experiment = ExperimentFactory.get_experiment("topic_modeling", **exp_params)

            if experiment:
                self.logger.info(f"Running experiment {run_name}")
                data_with_topics = experiment.run()
            else:
                self.logger.warning("Experiment factory returned None for topic_modeling")

        self.logger.info("Topic modelling pipeline complete.")

        if data_with_topics is not None:
            self.logger.info("Returning data with attached topic assignments")
            return data_with_topics
        else:
            self.logger.warning("No data to return from topic modelling pipeline")
            return X
