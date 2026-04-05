# experiments/topic_modeling_experiment.py
from .base import Experiment
from logs.logger import get_logger
from evaluators.factory import EvaluatorFactory
from visualisations.factory import VisualisationFactory
from models.factory import ModelFactory
from vectorizers.factory import VectorizerFactory
from embedding_models.factory import EmbeddingModelFactory
import mlflow
import pandas as pd
import os
from typing import Optional, Dict, Any
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np
import json
import inspect

TOPIC_ID = "_topic_id"
TOPIC_PROB = "topic_probability"


class TopicModelingExperiment(Experiment):
    """Generic topic modelling experiment wrapper.

    Expects text input column to be at X[self.combined_text_field_name] and full metadata preserved in X.
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
        combined_text_field_name: str = "__topic_input_text__",
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
        if not self.visualisations:
            self.logger.info("No visualisations specified for this experiment")
        # else:
        #     # add combined_text_field_name to each visualisation config for potential use in plot() method
        #     for viz_cfg in self.visualisations:
        #         viz_cfg["combined_text_field_name"] = combined_text_field_name

        self.save_path = save_path
        self.preprocessing_metadata = preprocessing_metadata or {}
        self.logger.info(f"Initialized TopicModelingExperiment with model '{self.model_name}' '")
        self.combined_text_field_name = combined_text_field_name
        self.logger.info(f"Combined text field for topic input: '{self.combined_text_field_name}'")

        # Instantiate evaluator
        #add combined_text_field_name to evaluator params for potential use in evaluation
        self.evaluator_params["combined_text_field_name"] = combined_text_field_name
        self.evaluator = EvaluatorFactory.get_evaluator(self.evaluator_name, **self.evaluator_params)

        # Placeholder: model will be created in run()
        self.model = None
        self.kwargs = kwargs



    def _attach_topics(self, topics, probs, topic_info):
        """Attach topic assignments to the original dataframe and return result df."""
        df = self.X.copy()

        df[TOPIC_ID] = topics

        # If probs is a 2D array (doc x topic distribution), take max probability for assigned topic; otherwise use as-is
        if probs is not None and len(probs) > 0 and isinstance(probs[0], (list, np.ndarray)):
            probs = np.max(probs, axis=1)

        df[TOPIC_PROB] = probs

        # topic_info is a dataframe with columns like 'Topic', 'Name', 'Count' for BERTopic; we want to merge this back to doc-level df
        if topic_info is not None:
            try:
                self.logger.info(f"topic_info columns: {topic_info.columns.tolist()}")
                # Select only useful columns
                topic_meta = topic_info[["Topic", "Name", "Count"]].copy()

                # Rename for clarity
                topic_meta = topic_meta.rename(columns={
                    "Topic": TOPIC_ID,
                    "Name": "topic_label",
                    "Count": "topic_count"
                })

                topic_words = {}
                topics_dict = self.model.get_topics()

                topic_words = {
                    topic_id: ", ".join(word for word, _ in words)
                    for topic_id, words in topics_dict.items()
                    if topic_id != -1
                }

                topic_meta["top_words"] = topic_meta[TOPIC_ID].map(topic_words)

                # Merge into document-level dataframe
                self.logger.info(f"Merging topic info into document-level dataframe on '{TOPIC_ID}'")
                df = df.merge(topic_meta, on=TOPIC_ID, how="left")

            except Exception as e:
                self.logger.warning(f"Could not merge topic info: {e}")

        return df

    def run(self):

        self.logger.info(f"Running topic modelling experiment '{self.name}' with model {self.model_name}")
        docs = self.X[self.combined_text_field_name].fillna("").tolist()
        self.logger.info(f"Extracted {len(docs)} documents for topic modeling from combined text field '{self.combined_text_field_name}'")
        topic_info = None

        with mlflow.start_run(run_name=self.name):
            # Build model via factory
            self.model = ModelFactory.get_model(self.model_name, **(self.model_params or {}))

            # For non-bertopic models, use embedding_model if provided via params or default to TF-IDF
            if not self.model_name.lower().startswith("bertopic"):
                pass
                #embedding_model_cfg = self.model_params.get("embedding_model") if isinstance(self.model_params, dict) else None
                # embedding_model_cfg = self.kwargs.get("embedding_model") if isinstance(self.kwargs, dict) else None
                # if embedding_model_cfg:
                #     vec_name = embedding_model_cfg.get("name")
                #     vec_params = embedding_model_cfg.get("params", {})
                #     embedding_model = VectorizerFactory.get_vectorizer(vec_name, **vec_params)
                #     X_vec = embedding_model.fit_transform(pd.Series(docs))
                # else:
                #     self.logger.warning("No embedding_model specified for non-BERTopic model; defaulting to TF-IDF with max_features=20000")
                #     # default TF-IDF
                #     vec = TfidfVectorizer(max_features=20000)
                #     X_vec = vec.fit_transform(docs)
                #
                # # Fit / transform using model wrapper
                # try:
                #     topics_matrix = self.model.fit_transform(X_vec)
                #     # topics_matrix: doc x topic distribution
                #     if hasattr(topics_matrix, "argmax"):
                #         import numpy as np
                #         top = np.argmax(topics_matrix, axis=1)
                #         probs = topics_matrix.max(axis=1).tolist()
                #         topics = top.tolist()
                #     else:
                #         topics = [int(t) for t in topics_matrix]
                #         probs = [1.0 for _ in topics]
                # except Exception as e:
                #     self.logger.error(f"Model fit_transform failed: {e}")
                #     raise
            else:
                # BERTopic via factory wrapper
                self.logger.info(f"Instantiating BERTopic model with params: {self.model_params}")
                embedding_model_cfg = self.kwargs.get("embedding_model")

                if embedding_model_cfg:
                    self.logger.info(f"Using Embedding Model '{embedding_model_cfg.get('name')}' for BERTopic embeddings with column '{embedding_model_cfg.get('column')}' and model_name '{embedding_model_cfg.get('model_name')}'")
                    embedding_model = EmbeddingModelFactory.get_embedding_model(
                        embedding_model_cfg.get("name"),
                        column=embedding_model_cfg.get("column"),
                        model_name=embedding_model_cfg.get("model_name")
                    )

                    self.logger.info(f"Fitting embedding_model on BERTopic input texts")
                    embeddings = embedding_model.transform(self.X)
                    self.logger.info(f"Embedding model produced embeddings with shape {embeddings.shape}")

                    topics, probs = self.model.fit_transform(docs, embeddings)
                    self.logger.info(f"BERTopic model fit_transform completed with embedding_model; assigned topics for {len(topics)} documents")
                else:
                    self.logger.info("No custom embedding_model specified for BERTopic; using default embedding model")
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

            # log parameters
            self._log_params()

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
                viz_params = {k: v for k, v in viz_cfg.items() if k != "name"}
                try:
                    self.logger.info(f"Instantiating visualisation '{viz_name}' with params: {viz_params}")
                    viz = VisualisationFactory.get_visualisation(viz_name, **viz_params)
                    if viz:
                        sig = inspect.signature(viz.plot)

                        if "model" in sig.parameters:
                            self.logger.info(f"Visualisation '{viz_name}' supports model parameter; passing model to plot()")
                            fig = viz.plot(result_df, model=self.model, topic_id = TOPIC_ID)
                        else:
                            self.logger.info(f"Visualisation '{viz_name}' does not support model parameter; calling plot() with dataframe only")
                            fig = viz.plot(result_df)

                        # Handle saving / MLflow
                        if isinstance(fig, list):
                            for path in fig:
                                mlflow.log_artifact(path)
                        else:
                            if hasattr(viz, "save") and self.save_path:
                                filename = viz_cfg.get("filename", f"{self.name}_{viz_name}.png")
                                full_path = os.path.join(self.save_path, filename)
                                viz.save(fig, full_path)
                                mlflow.log_artifact(full_path)
                except Exception as e:
                    self.logger.warning(f"Could not create viz {viz_name}: {e}")

        self.logger.info(f"Topic modelling experiment '{self.name}' complete")
        return result_df


    def _log_params(self):
        # model_params
        mlflow.log_param("model_name", self.model_name)
        for k, v in (self.model_params or {}).items():
            mlflow.log_param(f"model_param_{k}", v)

        embedding_model_cfg = self.kwargs.get("embedding_model") if isinstance(self.kwargs, dict) else None
        if embedding_model_cfg:
            for k, v in embedding_model_cfg.items():
                if isinstance(v, dict):
                    for sub_k, sub_v in v.items():
                        mlflow.log_param(f"embedding_model_{k}_{sub_k}", sub_v)
                else:
                    mlflow.log_param(f"embedding_model_{k}", v)
        # #
        # # # evaluator_params
        # # mlflow.log_param("evaluator_name", self.evaluator_name)
        # # for k, v in (self.evaluator_params or {}).items():
        # #     mlflow.log_param(f"evaluator_param_{k}", v)
        # #
        preprocessing_steps = getattr(self, "preprocessing_metadata", {})

        # If the steps are nested inside 'experiment_preprocessing', unwrap them
        if isinstance(preprocessing_steps, dict) and "experiment_preprocessing" in preprocessing_steps:
            preprocessing_steps = preprocessing_steps["experiment_preprocessing"]

        self.logger.info(
            f"ML Logging preprocessing steps for experiment '{self.name}'; found {len(preprocessing_steps)} steps")
        self.logger.info(f"Preprocessing steps: {preprocessing_steps}")

        for i, step in enumerate(preprocessing_steps):
            if isinstance(step, dict):
                name = step.get("name", str(step))
                applies_to = step.get("applies_to", "unknown")
                params = step.get("params", {})
                self.logger.info(
                    f"Processing preprocessing step {i}: name={name}, applies_to={applies_to}, params={params}")
                mlflow.log_param(f"preprocessing_{i}_name", name)
                mlflow.log_param(f"preprocessing_{i}_applies_to", applies_to)
                for param_k, param_v in params.items():
                    mlflow.log_param(f"preprocessing_{i}_param_{param_k}", param_v)
            else:
                self.logger.info(f"Processing preprocessing step {i} as string: {step}")
                mlflow.log_param(f"preprocessing_{i}_name", str(step))


        #visualisations = getattr(self, "visualisations", [])
        self.logger.info(
            f"ML Logging visualisations for experiment '{self.name}'; found {len(self.visualisations)} visualisations")
        self.logger.info(f"Visualisations: {self.visualisations}")
        for i, viz in enumerate(self.visualisations):
            self.logger.info(f"Processing visualisation {i}: {viz}")
            if isinstance(viz, dict):
                mlflow.log_param(f"visualisation_{i}_name", viz.get("name", str(viz)))
                for k, v in viz.items():
                    if k != "name":
                        mlflow.log_param(f"visualisation_{i}_{k}", v)
            else:
                # fallback if viz is just a string
                mlflow.log_param(f"visualisation_{i}_name", str(viz))