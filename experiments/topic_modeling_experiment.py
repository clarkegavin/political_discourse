# experiments/topic_modeling_experiment.py
from .base import Experiment
from logs.logger import get_logger
from evaluators.factory import EvaluatorFactory
from models.factory import ModelFactory
from vectorizers.factory import VectorizerFactory
from embedding_models.factory import EmbeddingModelFactory
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

        # Build model via factory
        self.model = ModelFactory.get_model(self.model_name, **(self.model_params or {}))

        # For non-bertopic models, use embedding_model if provided via params or default to TF-IDF
        if not self.model_name.lower().startswith("bertopic"):
            # If an embedding_model is provided via kwargs, use VectorizerFactory/EmbeddingModelFactory as appropriate
            embedding_model_cfg = self.kwargs.get("embedding_model")
            if embedding_model_cfg:
                vec_name = embedding_model_cfg.get("name")
                vec_params = embedding_model_cfg.get("params", {})
                embedding_model = VectorizerFactory.get_vectorizer(vec_name, **vec_params)
                X_vec = embedding_model.fit_transform(pd.Series(docs))
            else:
                # default TF-IDF
                vec = TfidfVectorizer(max_features=20000)
                X_vec = vec.fit_transform(docs)

            try:
                topics_matrix = self.model.fit_transform(X_vec)
                # topics_matrix: could be
                # - 1D list/array of topic ids
                # - 2D array/matrix of doc x topic distribution
                # - list of lists (handle defensively)
                import numpy as _np

                try:
                    arr = _np.array(topics_matrix)
                except Exception:
                    arr = None

                if arr is not None and hasattr(arr, 'ndim'):
                    if arr.ndim == 1:
                        # 1D: elements may still be lists/objects
                        if arr.dtype == object and len(arr) > 0 and isinstance(arr[0], (list, tuple, _np.ndarray)):
                            # take first element of each inner list as topic id
                            topics = [int(v[0]) if len(v) > 0 else -1 for v in arr]
                            probs = [1.0 for _ in topics]
                        else:
                            topics = arr.astype(int).tolist()
                            probs = [1.0 for _ in topics]
                    elif arr.ndim == 2:
                        top = _np.argmax(arr, axis=1)
                        probs = arr.max(axis=1).tolist()
                        topics = top.tolist()
                    else:
                        # unexpected shape: fall back to iteration
                        topics = []
                        for t in topics_matrix:
                            if isinstance(t, (list, tuple, _np.ndarray)):
                                topics.append(int(t[0]) if len(t) > 0 else -1)
                            else:
                                topics.append(int(t))
                        probs = [1.0 for _ in topics]
                else:
                    # not convertible to numpy array; iterate defensively
                    topics = []
                    for t in topics_matrix:
                        if isinstance(t, (list, tuple)):
                            topics.append(int(t[0]) if len(t) > 0 else -1)
                        else:
                            topics.append(int(t))
                    probs = [1.0 for _ in topics]
            except Exception as e:
                self.logger.error(f"Model fit_transform failed: {e}")
                raise
        else:
            # BERTopic via factory wrapper
            self.logger.info(f"Instantiating BERTopic model with params: {self.model_params}")
            embedding_model_cfg = self.model_params.get("embedding_model")
            self.logger.info(f"BERTopic embedding_model config: {embedding_model_cfg}")
            # tst_model_cfg = self.model_params.get("embedding_model") if self.model_params else None
            # self.logger.info(f"BERTopic model_params embedding_model config: {tst_model_cfg}")

            if embedding_model_cfg:
                self.logger.info(f"Using Embedding Model '{embedding_model_cfg.get('name')}' for BERTopic embeddings with column '{embedding_model_cfg.get('column')}' and model_name '{embedding_model_cfg.get('model_name')}'")
                embedding_model = EmbeddingModelFactory.get_embedding_model(
                    embedding_model_cfg.get("name"),
                    column=embedding_model_cfg.get("column"),
                    model_name=embedding_model_cfg.get("model_name"),
                    params = embedding_model_cfg.get("params", {})
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
            if topic_info is not None and self.save_path:
                self.logger.info(f"Saving topic info to CSV at {self.save_path}")
                csv_path = os.path.join(self.save_path or ".", f"{self.name}_topic_info.csv")
                os.makedirs(self.save_path, exist_ok=True)
                topic_info.to_csv(csv_path, index=False)
                artifacts = [csv_path]
            else:
                artifacts = []
        except Exception as e:
            self.logger.warning(f"Could not get topic info: {e}")
            artifacts = []

        # Attach back
        result_df = self._attach_topics(topics=topics, probs=probs, topic_info=topic_info)
        self.logger.info(f"Attached topic assignments to original dataframe; result shape: {result_df.shape}")

        # Experiments are responsible for returning results; evaluation & visualisation are handled by runners
        metadata = {
            "model_name": self.model_name,
            "model_params": self.model_params,
            "evaluator_name": self.evaluator_name,
            "evaluator_params": self.evaluator_params,
            "preprocessing_metadata": self.preprocessing_metadata,
            "combined_text_field_name": self.combined_text_field_name,
            "visualisations": self.visualisations,
            "topics": topics,
            "model": self.model,
        }

        return {"df": result_df, "metadata": metadata, "artifacts": artifacts}

    def _log_params(self):
        # Deprecated: experiments should not log directly to MLflow. Use collect_params() instead.
        return self.collect_params()

    def collect_params(self) -> dict:
        """Expose experiment params for external logging by ExperimentRunner."""
        return {
            "model_name": self.model_name,
            **(self.model_params or {}),
            "evaluator_name": self.evaluator_name,
            **(self.evaluator_params or {}),
            "visualisations": self.visualisations,
            "preprocessing": self.preprocessing_metadata,
        }
