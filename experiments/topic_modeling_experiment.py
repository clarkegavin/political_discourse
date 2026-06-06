# experiments/topic_modeling_experiment.py
from stopwords.provider import StopwordProvider
from .base import Experiment
from logs.logger import get_logger
from evaluators.factory import EvaluatorFactory
from models.factory import ModelFactory
from vectorizers.factory import VectorizerFactory
from embedding_models.factory import EmbeddingModelFactory
from reducers.factory import ReducerFactory
from representation_models.factory import RepresentationModelFactory
import pandas as pd
import os
from typing import Optional, Dict, Any
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np

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
        self.embedding_model_wrapper = None





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

    # Helper builders to keep run() tidy
    def _build_embedding_model(self):
        """Construct embedding model instance from model_params.embedding_model if present."""
        cfg = (self.model_params or {}).get("embedding_model")
        self.logger.info(f"Building embedding model with config: {cfg}")
        if not cfg:
            return None
        self.embedding_model_wrapper =  EmbeddingModelFactory.get_embedding_model(
            cfg.get("name"),
            column=cfg.get("column"),
            model_name=cfg.get("model_name"),
            params=cfg.get("params", {}),
        )

        # if factory returns a wrapper exposing underlying model, unwrap it for potential use in BERTopic kwargs; otherwise return the wrapper itself (e.g., if it implements fit_transform directly)

        if hasattr(self.embedding_model_wrapper, "model") and self.embedding_model_wrapper.model is not None:
            embedding_model= self.embedding_model_wrapper.model
        return self.embedding_model_wrapper, embedding_model


    def _build_vectorizer(self):
        """Construct a sklearn-style vectorizer for BERTopic or non-BERTopic usage.

        Returns a raw sklearn-like vectorizer (e.g., CountVectorizer/TfidfVectorizer) or None.
        If the factory returns a wrapper with `.vectorizer`, extract the underlying sklearn object.
        """
        cfg = (self.model_params or {}).get("vectorizer")
        if not cfg:
            return None
        name = cfg.get("name")
        params = cfg.get("params", {}).copy() or {}

        include_default = params.pop("include_default", False)
        include_procedural = params.pop("include_procedural", False)
        include_nltk = params.pop("include_nltk", False)

        custom_stopwords = StopwordProvider.get_stopwords(
            include_default= include_default,
            include_procedural = include_procedural,
            include_nltk= include_nltk
        )

        params["stop_words"] = sorted(list(custom_stopwords)) if custom_stopwords else None

        vec = VectorizerFactory.get_vectorizer(name, **params)
        # If factory returned a wrapper exposing underlying sklearn vectorizer, unwrap it
        if hasattr(vec, "vectorizer"):
            return getattr(vec, "vectorizer")
        return vec

    def _build_reducer(self):
        """Build reducer(s) from model_params.dimensionality_reduction_model if provided.

        ReducerFactory.get_reducer accepts a dict or list; return single reducer or None.
        """
        cfg = (self.model_params or {}).get("dimensionality_reduction_model")
        if not cfg:
            return None

        self.logger.info(f"Building reducer with config: {cfg}")
        # ReducerFactory.get_reducer returns list; accept first reducer if list returned
        reducer_list = ReducerFactory.get_reducers(cfg) if isinstance(cfg, list) or isinstance(cfg, dict) else []
        if not reducer_list or len(reducer_list) == 0:
            self.logger.warning(f"No reducers built from config: {cfg}")
            return None

        reducer =  reducer_list[0] # Take the first one

        # handle build pattern, e.g. PCA
        if hasattr(reducer, "build") and callable(reducer.build):
            self.logger.info(f"Building reducer using its build() method")
            reducer = reducer.build()
            self.logger.info(f"Called .build() on reducer")

        # unwrap if it has an underlying model attribute (e.g., for BERTopic compatibility)
        if hasattr(reducer, "model") and reducer.model is not None:
            self.logger.info(f"Reducer has underlying model: {reducer.model}")
            return reducer.model

        # for lazy reducers that only build during fit/transform, return the wrapper itself (e.g., if it implements fit_transform directly)
        self.logger.info(f"Reducer does not have an underlying model attribute; returning wrapper instance")
        return reducer

    def _build_clusterer(self):
        """Build clusterer (e.g., HDBSCAN) using ModelFactory with config under model_params.clusterer."""
        cfg = (self.model_params or {}).get("clusterer")
        if not cfg:
            return None

        if not isinstance(cfg, dict):
            self.logger.warning(f"Clusterer config should be a dict with 'name' and optional 'params'. Got: {cfg}")
            return None

        name = cfg.get("name")
        params = cfg.get("params", {}) or {}

        self.logger.info(f"Building clusterer '{name}' with params: {params}")

        # ModelFactory.get_model will instantiate the clusterer wrapper/class
        clusterer_wrapper = ModelFactory.get_model(name, **params)

        if hasattr(clusterer_wrapper, "build") and callable(clusterer_wrapper.build):
            self.logger.info(f"Building clusterer '{name}' using its build() method")
            clusterer_wrapper = clusterer_wrapper.build()
            self.logger.info(f"Called .build() on clusterer '{name}'")



        self.logger.info(f"Built clusterer '{name}' with params: {params}")
        # If factory returned a wrapper exposing underlying sklearn model, unwrap it
        if hasattr(clusterer_wrapper, "model") and clusterer_wrapper.model is not None:
            self.logger.info(f"Clusterer '{name}' has underlying model: {clusterer_wrapper.model}")
            return clusterer_wrapper.model

        # If no underlying model attribute, return the wrapper itself (e.g., if it implements fit_predict directly)
        self.logger.info(f"Clusterer '{name}' does not have an underlying model attribute; returning wrapper instance")
        return clusterer_wrapper

    def _build_representation_model(self):
        """ Build representation """
        cfg = (self.model_params or {}).get("representation_model")
        if not cfg:
            return None

        if not isinstance(cfg, dict):
            self.logger.warning(f"Representation model config should be a dict with 'name' and optional 'params'. Got: {cfg}")
            return None

        name = cfg.get("name")
        params = cfg.get("params", {}) or {}
        self.logger.info(f"Building representation model '{name}' with params: {params}")

        representation_wrapper = RepresentationModelFactory.create_representation_model(name, **params)

        if hasattr(representation_wrapper, "build") and callable(representation_wrapper.build):
            self.logger.info(f"Building representation model '{name}' using its build() method")
            representation_wrapper = representation_wrapper.build()
            self.logger.info(f"Called .build() on representation model '{name}'")

        self.logger.info(f"Built representation model '{name}' with params: {params}")
        # If factory returned a wrapper exposing underlying model, unwrap it
        if hasattr(representation_wrapper, "representation_model") and representation_wrapper.representation_model is not None:
            self.logger.info(f"Representation model '{name}' has underlying model: {representation_wrapper.representation_model}")
            model = representation_wrapper.representation_model
            self.logger.info(f"Representation Model Attributes: {vars(model)}")
            return model

        # If no underlying model attribute, return the wrapper itself (e.g., if it implements fit_transform directly)
        self.logger.info(f"Representation model '{name}' does not have an underlying model attribute; returning wrapper instance")
        return representation_wrapper

    def _build_ctfidf_model(self):
        """Build a Class-based TF-IDF model for BERTopic if specified in model_params.ctfidf."""
        cfg = (self.model_params or {}).get("ctfidf")
        if not cfg:
            return None

        if not isinstance(cfg, dict):
            self.logger.warning(f"c-TF-IDF config should be a dict with 'name' and optional 'params'. Got: {cfg}")
            return None

        name = cfg.get("name")
        params = cfg.get("params", {}) or {}

        self.logger.info(f"Building c-TF-IDF model '{name}' with params: {params}")

        ctfidf_wrapper = ModelFactory.get_model(name, **params)

        if hasattr(ctfidf_wrapper, "build") and callable(ctfidf_wrapper.build):
            self.logger.info(f"Building c-TF-IDF model '{name}' using its build() method")
            ctfidf_wrapper = ctfidf_wrapper.build()
            self.logger.info(f"Called .build() on c-TF-IDF model '{name}'")

        self.logger.info(f"Built c-TF-IDF model '{name}' with params: {params}")
        # If factory returned a wrapper exposing underlying model, unwrap it
        if hasattr(ctfidf_wrapper, "model") and ctfidf_wrapper.model is not None:
            self.logger.info(f"c-TF-IDF model '{name}' has underlying model: {ctfidf_wrapper.model}")
            return ctfidf_wrapper.model

        # If no underlying model attribute, return the wrapper itself (e.g., if it implements fit_transform directly)
        self.logger.info(f"CTFIDF model '{name}' does not have an underlying model attribute; returning wrapper instance")
        return ctfidf_wrapper

    def _collect_bertopic_kwargs(self):
        """Collect and return kwargs to pass into BERTopicModel via ModelFactory.

        This will map known keys to the argument names BERTopic expects:
         - vectorizer -> vectorizer_model
         - dimensionality_reduction_model -> umap_model
         - clusterer -> hdbscan_model
        It will also remove embedding_model from the kwargs because embeddings are computed
        and passed to fit_transform separately.
        """
        model_kwargs = dict(self.model_params or {})

        # Build and inject vectorizer
        vec = self._build_vectorizer()
        if vec is not None:
            model_kwargs.pop("vectorizer", None)
            model_kwargs["vectorizer_model"] = vec

        # Build and inject reducer (UMAP-like)
        reducer = self._build_reducer()
        if reducer is not None:
            model_kwargs.pop("dimensionality_reduction_model", None)
            # BERTopic expects umap_model
            model_kwargs["umap_model"] = reducer

        # Build and inject clusterer (HDBSCAN-like)
        clusterer = self._build_clusterer()
        if clusterer is not None:
            model_kwargs.pop("clusterer", None)
            # BERTopic expects hdbscan_model
            model_kwargs["hdbscan_model"] = clusterer
        else:
            self.logger.warning("No clusterer built; BERTopic will use default HDBSCAN clusterer")

        # Representation Model (e.g., KeyBERT/MMR) is not directly supported by BERTopic, but we can build it here for potential use in evaluation or visualisation. It will not be passed into BERTopicModel kwargs but can be included in metadata.
        representation_model = self._build_representation_model()
        if representation_model is not None:
            model_kwargs.pop("representation_model", None)
            model_kwargs["representation_model"] = representation_model

        # Remove embedding_model config because we compute embeddings in the experiment and pass them
        # if "embedding_model" in model_kwargs:
        #     model_kwargs.pop("embedding_model", None)

        # Retain the embedding model for topic representation model usage
        self.embedding_model_wrapper, embedding_model = self._build_embedding_model()
        if embedding_model is not None:
            model_kwargs["embedding_model"] = embedding_model
            self.logger.info(f"Kept embedding_model in constructor: {type(embedding_model).__name__}")
        else:
            model_kwargs.pop("embedding_model", None)

        return model_kwargs

    def run(self):
        self.logger.info(f"Running topic modelling experiment '{self.name}' with model {self.model_name}")
        docs = self.X[self.combined_text_field_name].fillna("").tolist()
        self.logger.info(f"Extracted {len(docs)} documents for topic modeling from combined text field '{self.combined_text_field_name}'")
        topic_info = None

        # Only support BERTopic models here. Log and return if another model is requested.
        if not self.model_name.lower().startswith("bertopic"):
            self.logger.warning(f"TopicModelingExperiment currently supports BERTopic models only. '{self.model_name}' is not supported.")
            # Return a no-op result with original dataframe
            return {"df": self.X.copy(), "metadata": {"model_name": self.model_name}, "artifacts": []}

        # BERTopic flow
        self.logger.info(f"Preparing BERTopic model with params: {self.model_params}")
        # Build model kwargs by collecting components via helpers
        model_kwargs = self._collect_bertopic_kwargs()
        self.logger.info(f"Collected BERTopic kwargs: {model_kwargs}")

        # Build embedding model and compute embeddings if present
        # embedding_model_wrapper, embedding_model = self._build_embedding_model()
        if self.embedding_model_wrapper is not None:
            self.logger.info(f"Using embedding model: {self.embedding_model_wrapper}")
            embeddings = self.embedding_model_wrapper.transform(self.X)
            self.logger.info(f"Embedding model produced embeddings with shape {embeddings.shape}")
        else:
            embeddings = None

        # Instantiate BERTopicModel wrapper via ModelFactory with the collected kwargs
        self.logger.info(f"Instantiating BERTopicModel with collected kwargs: {model_kwargs}")
        self.model = ModelFactory.get_model(self.model_name, **model_kwargs)
        self.logger.info(f"Instantiated BERTopicModel: {self.model}")

        # Fit/transform using docs and optional embeddings
        if embeddings is not None:
            self.logger.info("Fitting BERTopicModel with embeddings")
            topics, probs = self.model.fit_transform(docs, embeddings)
        else:
            self.logger.info("Fitting BERTopicModel without embeddings")
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
