# experiments/topic_modeling_experiment.py
from .base import Experiment
from logs.logger import get_logger
from evaluators.factory import EvaluatorFactory
from visualisations.factory import VisualisationFactory
from models.factory import ModelFactory
from vectorizers.factory import VectorizerFactory
import mlflow
import pandas as pd
import os
import importlib
from typing import Optional, Dict, Any

class TopicModelingExperiment(Experiment):
    """Generic topic modelling experiment wrapper.

    Expects text input column to be at X['__topic_input_text__'] and full metadata preserved in X.
    """

    def __init__(
        self,
        name: str,
        model_name: str,
        evaluator_name: str,
        X: pd.DataFrame,
        model_params: Optional[Dict[str, Any]] = None,
        evaluator_params: Optional[Dict[str, Any]] = None,
        mlflow_tracking: bool = True,
        mlflow_experiment: Optional[str] = None,
        visualisations: Optional[list] = None,
        save_path: Optional[str] = None,
        preprocessing_metadata: Optional[Dict] = None,
        **kwargs,
    ):
        super().__init__(name, mlflow_tracking, mlflow_experiment)
        self.logger = get_logger(f"TopicModelingExperiment.{name}")
        self.name = name
        self.model_name = model_name
        self.model_params = model_params or {}
        self.evaluator_name = evaluator_name
        self.evaluator_params = evaluator_params or {}
        self.X = X
        self.visualisations = visualisations or []
        self.save_path = save_path
        self.preprocessing_metadata = preprocessing_metadata or {}
        self.logger.info(f"Initialized TopicModelingExperiment with model '{self.model_name}' '")

        # Instantiate evaluator
        self.evaluator = EvaluatorFactory.get_evaluator(self.evaluator_name, **self.evaluator_params)

        # Placeholder: model will be created in run()
        self.model = None

    # def _build_model(self):
    #     """Create topic model object based on model_name. Keep it extensible."""
    #     # Lazy import to avoid heavy dependencies unless used
    #     if self.model_name.lower().startswith("bertopic"):
    #         try:
    #             bertopic_mod = importlib.import_module("bertopic")
    #             BERTopic = getattr(bertopic_mod, "BERTopic")
    #         except Exception as e:
    #             self.logger.error(f"BERTopic not available: {e}")
    #             raise
    #         params = dict(self.model_params)
    #         # map to BERTopic parameter names if required
    #         return BERTopic(**params)
    #     elif self.model_name.lower() in ("lda", "nmf"):
    #         # simple sklearn implementations
    #         from sklearn.decomposition import LatentDirichletAllocation, NMF
    #         if self.model_name.lower() == "lda":
    #             return LatentDirichletAllocation(**(self.model_params or {}))
    #         return NMF(**(self.model_params or {}))
    #     else:
    #         raise ValueError(f"Unknown topic model: {self.model_name}")

    def _attach_topics(self, topics, topic_info, probs):
        """Attach topic assignments to the original dataframe and return result df."""
        df = self.X.copy()
        df["topic"] = topics
        df["topic_info"] = topic_info
        df["topic_probability"] = probs
        return df

    def run(self):
        self.logger.info(f"Running topic modelling experiment '{self.name}' with model {self.model_name}")
        docs = self.X["__topic_input_text__"].fillna("").tolist()

        with mlflow.start_run(run_name=self.name):
            # Build model via factory
            self.model = ModelFactory.get_model(self.model_name, **(self.model_params or {}))

            # For non-bertopic models, use vectorizer if provided via params or default to TF-IDF
            if not self.model_name.lower().startswith("bertopic"):
                vec_cfg = self.model_params.get("vectorizer") if isinstance(self.model_params, dict) else None
                if vec_cfg:
                    vec_name = vec_cfg.get("name")
                    vec_params = vec_cfg.get("params", {})
                    vectorizer = VectorizerFactory.get_vectorizer(vec_name, **vec_params)
                    X_vec = vectorizer.fit_transform(pd.Series(docs))
                else:
                    # default TF-IDF
                    from sklearn.feature_extraction.text import TfidfVectorizer
                    vec = TfidfVectorizer(max_features=20000)
                    X_vec = vec.fit_transform(docs)

                # Fit / transform using model wrapper
                try:
                    topics_matrix = self.model.fit_transform(X_vec)
                    # topics_matrix: doc x topic distribution
                    if hasattr(topics_matrix, "argmax"):
                        import numpy as np
                        top = np.argmax(topics_matrix, axis=1)
                        probs = topics_matrix.max(axis=1).tolist()
                        topics = top.tolist()
                    else:
                        topics = [int(t) for t in topics_matrix]
                        probs = [1.0 for _ in topics]
                except Exception as e:
                    self.logger.error(f"Model fit_transform failed: {e}")
                    raise
            else:
                # BERTopic via factory wrapper
                topics, probs = self.model.fit_transform(docs)
                # get topic info
                try:
                    topic_info = self.model.get_topic_info()
                    mlflow.log_artifact(topic_info.to_csv(index=False), artifact_path=f"{self.name}_topic_info.csv")
                except Exception as e:
                    self.logger.warning(f"Could not get topic info: {e}")

            # Attach back
            result_df = self._attach_topics(topics, topic_info, probs)

            # Log model parameters and basic metrics
            mlflow.log_param("model_name", self.model_name)
            for k, v in (self.model_params or {}).items():
                mlflow.log_param(f"model_param_{k}", v)

            # Evaluate
            try:
                metrics = self.evaluator.evaluate(self.X, topics, self.model)
                for k, v in metrics.items():
                    if isinstance(v, (int, float)):
                        mlflow.log_metric(k, v)
                    else:
                        # save dict/list metrics as artifacts
                        import json
                        filename = f"{k}.json"
                        with open(filename, "w") as f:
                            json.dump(v, f)
                        mlflow.log_artifact(filename)
            except Exception as e:
                self.logger.warning(f"Evaluator failed: {e}")
                metrics = {}

            # Save artifacts
            if self.save_path:
                os.makedirs(self.save_path, exist_ok=True)
                # Save per-document topics
                doc_path = os.path.join(self.save_path, f"{self.name}_doc_topics.parquet")
                try:
                    result_df.to_parquet(doc_path)
                    mlflow.log_artifact(doc_path)
                except Exception as e:
                    self.logger.warning(f"Could not save doc topics: {e}")

            # Visualisations
            self.logger.info(f"Generating visualisations for experiment '{self.name}'")
            for viz_cfg in self.visualisations:
                self.logger.info(f"Creating visualisation with config: {viz_cfg}")
                viz_name = viz_cfg.get("name")
                viz_params = viz_cfg.get("params", {})
                try:
                    viz = VisualisationFactory.get_visualisation(viz_name, **viz_params)
                    if viz:
                        fig = viz.plot(result_df)
                        # save via viz if possible
                        if hasattr(viz, "save") and self.save_path:
                            viz.save(fig, os.path.join(self.save_path, f"{self.name}_{viz_name}.png"))
                except Exception as e:
                    self.logger.warning(f"Could not create viz {viz_name}: {e}")

        self.logger.info(f"Topic modelling experiment '{self.name}' complete")
        return result_df
