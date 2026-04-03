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
from typing import Optional, Dict, Any
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np
import json

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
        self.logger = get_logger(self.__class__.__name__)
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



    def _attach_topics(self, topics, probs, topic_info):
        """Attach topic assignments to the original dataframe and return result df."""
        df = self.X.copy()

        df["_topic_id"] = topics

        # If probs is a 2D array (doc x topic distribution), take max probability for assigned topic; otherwise use as-is
        if probs is not None and len(probs) > 0 and isinstance(probs[0], (list, np.ndarray)):
            probs = np.max(probs, axis=1)

        df["topic_probability"] = probs

        # topic_info is a dataframe with columns like 'Topic', 'Name', 'Count' for BERTopic; we want to merge this back to doc-level df
        if topic_info is not None:
            try:
                self.logger.info(f"topic_info columns: {topic_info.columns.tolist()}")
                # Select only useful columns
                topic_meta = topic_info[["Topic", "Name", "Count"]].copy()

                # Rename for clarity
                topic_meta = topic_meta.rename(columns={
                    "Topic": "_topic_id",
                    "Name": "topic_label",
                    "Count": "topic_count"
                })

                topic_words = {}
                topics_dict = self.model.get_topics()

                # for topic in topic_meta["_topic_id"]:
                #     if topic == -1:
                #         continue
                #
                #     words = topics_dict.get(topic)
                #
                #     if words:
                #         topic_words[topic] = ", ".join([word for word, _ in words])

                topic_words = {
                    topic_id: ", ".join(word for word, _ in words)
                    for topic_id, words in topics_dict.items()
                    if topic_id != -1
                }

                topic_meta["top_words"] = topic_meta["_topic_id"].map(topic_words)

                # Merge into document-level dataframe
                self.logger.info(f"Merging topic info into document-level dataframe on '_topic_id'")
                df = df.merge(topic_meta, on="_topic_id", how="left")

            except Exception as e:
                self.logger.warning(f"Could not merge topic info: {e}")

        return df

    def run(self):

        self.logger.info(f"Running topic modelling experiment '{self.name}' with model {self.model_name}")
        docs = self.X["__topic_input_text__"].fillna("").tolist()
        topic_info = None

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
                    self.logger.warning("No vectorizer specified for non-BERTopic model; defaulting to TF-IDF with max_features=20000")
                    # default TF-IDF
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
                    if topic_info is not None:
                        csv_path = os.path.join(self.save_path or ".", f"{self.name}_topic_info.csv")
                        topic_info.to_csv(csv_path, index=False)
                        mlflow.log_artifact(csv_path)

                    #mlflow.log_artifact(topic_info.to_csv(index=False), artifact_path=f"{self.name}_topic_info.csv")
                except Exception as e:
                    self.logger.warning(f"Could not get topic info: {e}")

            # Attach back
            result_df = self._attach_topics(topics=topics, probs=probs, topic_info=topic_info)
            self.logger.info(f"Attached topic assignments to original dataframe; result shape: {result_df.shape}")

            # Log model parameters and basic metrics
            mlflow.log_param("model_name", self.model_name)
            for k, v in (self.model_params or {}).items():
                mlflow.log_param(f"model_param_{k}", v)

            # Evaluate
            self.logger.info(f"Evaluating topic model assignments for experiment '{self.name}' using evaluator '{self.evaluator_name}'")
            try:
                metrics = self.evaluator.evaluate(self.X, topics, self.model)
                self.logger.info(f"Evaluator returned metrics: {metrics}")
                for k, v in metrics.items():
                    self.logger.info(f"Logging metric '{k}': {v}")
                    if isinstance(v, (int, float)):
                        mlflow.log_metric(k, v)
                    else:
                        # save dict/list metrics as artifacts
                        self.logger.info(f"Saving non-numeric metric '{k}' as artifact")
                        filename = f"{k}.json"
                        with open(filename, "w") as f:
                            json.dump(v, f)
                        mlflow.log_artifact(filename)
            except Exception as e:
                self.logger.warning(f"Evaluator failed: {e}")


            self.logger.info(f"Completed evaluation for experiment '{self.name}'")

            # Save artifacts
            if self.save_path:
                self.logger.info(f"Saving artifacts for experiment '{self.name}' to {self.save_path}")
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
                #viz_params = viz_cfg.get("params", {})
                viz_params = {k: v for k, v in viz_cfg.items() if k != "name"}
                try:
                    viz = VisualisationFactory.get_visualisation(viz_name, **viz_params)
                    if viz:
                        fig = viz.plot(result_df)
                        # save via viz if possible
                        if hasattr(viz, "save") and self.save_path:
                            filename = viz_cfg.get("filename")
                            if not filename:
                                filename = f"{self.name}_{viz_name}.png"
                            full_path = os.path.join(self.save_path, filename)
                            viz.save(fig, full_path)
                            mlflow.log_artifact(full_path)
                            #viz.save(fig, os.path.join(self.save_path, f"{self.name}_{viz_name}.png"))
                except Exception as e:
                    self.logger.warning(f"Could not create viz {viz_name}: {e}")

        self.logger.info(f"Topic modelling experiment '{self.name}' complete")
        return result_df



    # def run(self):
    #     import time, os
    #
    #     self.logger.info(f"Running topic modelling experiment '{self.name}' with model {self.model_name} and PID = {os.getpid()}" )
    #
    #     docs = self.X["__topic_input_text__"].fillna("").tolist()
    #     topic_info = None
    #
    #     with mlflow.start_run(run_name=self.name):
    #         # Build model
    #         self.model = ModelFactory.get_model(self.model_name, **(self.model_params or {}))
    #
    #         if not self.model_name.lower().startswith("bertopic"):
    #             # ... your non-bertopic code (keep as is) ...
    #             pass
    #         else:
    #             topics, probs = self.model.fit_transform(docs)
    #
    #             try:
    #                 topic_info = self.model.get_topic_info()
    #                 if topic_info is not None:
    #                     csv_path = os.path.join(self.save_path or ".", f"{self.name}_topic_info.csv")
    #                     topic_info.to_csv(csv_path, index=False)
    #                     mlflow.log_artifact(csv_path)
    #             except Exception as e:
    #                 self.logger.warning(f"Could not get topic info: {e}")
    #
    #         # Attach topics
    #         result_df = self._attach_topics(topics=topics, probs=probs, topic_info=topic_info)
    #         self.logger.info(f"Attached topic assignments to original dataframe; result shape: {result_df.shape}")
    #
    #         # Log params
    #         mlflow.log_param("model_name", self.model_name)
    #         for k, v in (self.model_params or {}).items():
    #             mlflow.log_param(f"model_param_{k}", v)
    #
    #         # Evaluate
    #         self.logger.info(
    #             f"Evaluating topic model assignments for experiment '{self.name}' using evaluator '{self.evaluator_name}'")
    #         metrics = self.evaluator.evaluate(self.X, topics, self.model)
    #
    #         self.logger.info(f"Evaluator returned metrics: {metrics}")
    #         for k, v in metrics.items():
    #             if isinstance(v, (int, float)):
    #                 mlflow.log_metric(k, v)
    #             else:
    #                 filename = f"{k}.json"
    #
    #                 with open(filename, "w") as f:
    #                     json.dump(v, f)
    #                 mlflow.log_artifact(filename)
    #
    #         # Save artifacts, visualisations, etc. (keep your existing code here)
    #
    #     self.logger.info(f"Topic modelling experiment '{self.name}' complete")
    #
    #     return result_df
