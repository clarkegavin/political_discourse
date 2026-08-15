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
from pathlib import Path
import json
import torch
import hashlib
import time
from datetime import datetime, timezone
from llm.prompts.topic_theme import (
    build_topic_theme_prompt,
    PROMPT_VERSION,
)


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
        dataset_name: Optional[str] = None,
        model_params: Optional[Dict[str, Any]] = None,
        evaluator_params: Optional[Dict[str, Any]] = None,
        mlflow_tracking: bool = True,
        mlflow_experiment: Optional[str] = None,
        visualisations: Optional[list] = None,
        topic_outputs: Optional[Dict[str, Any]] = None,
        llm = None,
        save_path: Optional[str] = None,
        preprocessing_metadata: Optional[Dict] = None,
        #combined_text_field_name: str = "__topic_input_text__",
        representation_text_field: Optional[str] = None,
        **kwargs,
    ):


        super().__init__(name, mlflow_tracking, mlflow_experiment)
        self.logger = get_logger(self.__class__.__name__)
        self.name = name
        self.model_name = model_name
        self.dataset_name = dataset_name
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

        self.topic_outputs = topic_outputs or {}

        self.topic_outputs_enabled = self.topic_outputs.get(
            "enabled",
            False
        )

        self.save_topic_info = self.topic_outputs.get(
            "save_topic_info",
            self.topic_outputs_enabled
        )

        self.save_topic_hierarchy = self.topic_outputs.get(
            "save_topic_hierarchy",
            self.topic_outputs_enabled
        )

        self.logger.info(
            "Topic outputs configuration: enabled=%s, "
            "save_topic_info=%s, "
            "save_topic_hierarchy=%s",
            self.topic_outputs_enabled,
            self.save_topic_info,
            self.save_topic_hierarchy
        )

        self.llm_config = llm or {}
        self.llm_enabled = self.llm_config.get(
            "enabled",
            False
        )

        self.llm_max_topics = self.llm_config.get(
            "max_topics",
            None
        )

        self.llm_request_delay = self.llm_config.get(
            "request_delay_seconds",
            1.0
        )

        self.llm_max_retries = self.llm_config.get(
            "max_retries",
            5
        )

        self.llm_retry_delay = self.llm_config.get(
            "retry_delay",
            2.0
        )

        self.llm_max_representative_docs = self.llm_config.get(
            "max_representative_docs",
            3
        )

        self.llm_max_doc_characters = self.llm_config.get(
            "max_doc_characters",
            500
        )

        self.llm_max_ancestors = self.llm_config.get(
            "max_ancestors",
            3
        )


        self.logger.info(f"LLM Configurations: max_topics={self.llm_max_topics}, "
                         f"request_delay={self.llm_request_delay}, max_retries={self.llm_max_retries}, "
                         f"retry_delay={self.llm_retry_delay}, max_representative_docs={self.llm_max_representative_docs}, "
                         f"max_doc_characters={self.llm_max_doc_characters}, "
                         f"max_ancestors={self.llm_max_ancestors}")

        self.save_path = save_path

        self.preprocessing_metadata = preprocessing_metadata or {}
        self.logger.info(f"Initialized TopicModelingExperiment with model '{self.model_name}' '")
        #self.combined_text_field_name = combined_text_field_name
        self.representation_text_field = representation_text_field
        #self.logger.info(f"Combined text field for topic input: '{self.combined_text_field_name}'")

        # Instantiate evaluator
        #add combined_text_field_name to evaluator params for potential use in evaluation
        #self.evaluator_params["combined_text_field_name"] = combined_text_field_name
        self.evaluator_params["evaluation_field_name"] = self.representation_text_field
        self.evaluator = EvaluatorFactory.get_evaluator(self.evaluator_name, **self.evaluator_params)

        # Placeholder: model will be created in run()
        self.model = None
        self.kwargs = kwargs
        self.embedding_model_wrapper = None
        self._extract_embedding_cache_params()





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

    def _build_topic_hierarchy(self, documents):
        """
        Generate the BERTopic hierarchical topic structure.

        Unwraps the BERTopicModel wrapper before calling
        BERTopic.hierarchical_topics().
        """

        if self.model is None:
            self.logger.warning(
                "Cannot build topic hierarchy: BERTopic model is not available"
            )
            return None

        try:

            # Unwrap the BERTopicModel wrapper
            bertopic_model = getattr(
                self.model,
                "model",
                self.model
            )

            self.logger.info(
                "Underlying BERTopic model type: %s",
                type(bertopic_model).__name__
            )

            if not hasattr(
                    bertopic_model,
                    "hierarchical_topics"
            ):
                self.logger.warning(
                    "Underlying model does not expose "
                    "'hierarchical_topics'"
                )
                return None

            self.logger.info(
                "Generating BERTopic hierarchical topic structure"
            )

            hierarchical_topics = (
                bertopic_model.hierarchical_topics(
                    documents
                )
            )

            if hierarchical_topics is None:
                self.logger.warning(
                    "BERTopic returned no hierarchical topics"
                )
                return None

            self.logger.info(
                "Generated topic hierarchy with %d rows "
                "and columns: %s",
                len(hierarchical_topics),
                hierarchical_topics.columns.tolist()
            )

            self.logger.info(
                "Topic hierarchy preview:\n%s",
                hierarchical_topics.head().to_string()
            )

            return hierarchical_topics

        except Exception as e:

            self.logger.exception(
                f"Could not generate topic hierarchy: {e}"
            )

            return None

    def _build_topic_hierarchy_nodes(
            self,
            topic_info,
            topic_hierarchy
    ):
        """
        Build a relational representation of all nodes in the BERTopic
        hierarchy.

        Leaf nodes come from BERTopic topic_info.
        Parent nodes come from BERTopic hierarchical_topics().

        Returns
        -------
        pd.DataFrame
            One row per hierarchy node.
        """

        self.logger.info(
            "Building topic hierarchy nodes"
        )

        if topic_info is None or topic_info.empty:
            self.logger.warning(
                "Cannot build hierarchy nodes: topic_info is empty"
            )
            return None

        if topic_hierarchy is None or topic_hierarchy.empty:
            self.logger.warning(
                "Cannot build hierarchy nodes: topic_hierarchy is empty"
            )
            return None

        try:

            nodes = []

            # ---------------------------------------------------------
            # Identify hierarchy parent IDs
            # ---------------------------------------------------------

            hierarchy_parent_ids = set(
                topic_hierarchy["Parent_ID"]
                .dropna()
                .astype(int)
                .tolist()
            )

            # ---------------------------------------------------------
            # Leaf nodes
            #
            # These are the actual BERTopic topics from topic_info.
            # Topic -1 is the BERTopic outlier topic and is retained
            # as a leaf node.
            # ---------------------------------------------------------

            for _, row in topic_info.iterrows():
                topic_id = int(row["Topic"])

                nodes.append({
                    "node_id": topic_id,
                    "node_type": "leaf",
                    "topic_label": row.get("Name"),
                    "topic_count": row.get("Count"),
                    "top_words": row.get("Representation"),
                    "representative_docs": row.get(
                        "Representative_Docs"
                    ),
                    "parent_id": None,
                    "child_left_id": None,
                    "child_right_id": None,
                    "distance": None,
                })

            # ---------------------------------------------------------
            # Parent nodes
            #
            # Every row in hierarchical_topics represents a merge:
            #
            #       Child_Left + Child_Right
            #                  ↓
            #              Parent_ID
            #
            # Therefore each Parent_ID becomes a node.
            # ---------------------------------------------------------

            for _, row in topic_hierarchy.iterrows():
                parent_id = int(row["Parent_ID"])

                nodes.append({
                    "node_id": parent_id,
                    "node_type": "parent",
                    "topic_label": row.get("Parent_Name"),
                    "topic_count": None,
                    "top_words": None,
                    "representative_docs": None,
                    "parent_id": None,
                    "child_left_id": (
                        int(row["Child_Left_ID"])
                        if pd.notna(row["Child_Left_ID"])
                        else None
                    ),
                    "child_right_id": (
                        int(row["Child_Right_ID"])
                        if pd.notna(row["Child_Right_ID"])
                        else None
                    ),
                    "distance": (
                        float(row["Distance"])
                        if pd.notna(row["Distance"])
                        else None
                    ),
                })

            # ---------------------------------------------------------
            # Create DataFrame
            # ---------------------------------------------------------

            nodes_df = pd.DataFrame(nodes)

            # ---------------------------------------------------------
            # Build parent relationships
            #
            # If a node appears as either child of a hierarchy merge,
            # its parent is that merge's Parent_ID.
            # ---------------------------------------------------------

            parent_relationships = {}

            for _, row in topic_hierarchy.iterrows():

                parent_id = int(row["Parent_ID"])

                for child_column in [
                    "Child_Left_ID",
                    "Child_Right_ID"
                ]:

                    child_id = row[child_column]

                    if pd.notna(child_id):
                        child_id = int(child_id)

                        parent_relationships[child_id] = parent_id

            nodes_df["parent_id"] = (
                nodes_df["node_id"]
                .map(parent_relationships)
            )

            # ---------------------------------------------------------
            # Ensure sensible ordering:
            #
            # leaf topics first, followed by generated parent nodes.
            # ---------------------------------------------------------

            nodes_df["_node_type_order"] = (
                nodes_df["node_type"]
                .map({
                    "leaf": 0,
                    "parent": 1
                })
            )

            nodes_df = (
                nodes_df
                .sort_values(
                    ["_node_type_order", "node_id"]
                )
                .drop(
                    columns=["_node_type_order"]
                )
                .reset_index(drop=True)
            )

            # ---------------------------------------------------------
            # Logging / validation
            # ---------------------------------------------------------

            leaf_count = (
                nodes_df["node_type"]
                .eq("leaf")
                .sum()
            )

            parent_count = (
                nodes_df["node_type"]
                .eq("parent")
                .sum()
            )

            self.logger.info(
                "Generated topic hierarchy nodes: "
                "%d total nodes (%d leaf, %d parent)",
                len(nodes_df),
                leaf_count,
                parent_count
            )

            self.logger.info(
                "Topic hierarchy nodes columns: %s",
                nodes_df.columns.tolist()
            )

            self.logger.info(
                "Topic hierarchy nodes preview:\n%s",
                nodes_df.head(10).to_string()
            )

            # ---------------------------------------------------------
            # Validate parent/child relationships
            # ---------------------------------------------------------

            sample_parent_id = 1177

            sample_parent = nodes_df[
                nodes_df["node_id"] == sample_parent_id
                ]

            if not sample_parent.empty:

                self.logger.info(
                    "Sample parent node %s:\n%s",
                    sample_parent_id,
                    sample_parent.to_string(index=False)
                )

                child_ids = []

                for column in [
                    "child_left_id",
                    "child_right_id"
                ]:
                    child_id = sample_parent.iloc[0][column]

                    if pd.notna(child_id):
                        child_ids.append(int(child_id))

                for child_id in child_ids:
                    child = nodes_df[
                        nodes_df["node_id"] == child_id
                        ]

                    self.logger.info(
                        "Child node %s:\n%s",
                        child_id,
                        child.to_string(index=False)
                    )

            return nodes_df

        except Exception as e:

            self.logger.exception(
                "Could not build topic hierarchy nodes: %s",
                e
            )

            return None

    # Helper builders to keep run() tidy
    def _build_embedding_model(self):
        """Construct embedding model instance from model_params.embedding_model if present."""
        cfg = (self.model_params or {}).get("embedding_model")
        self.logger.info(f"Building embedding model with config: {cfg}")
        if not cfg:
            # Always return a tuple for consistent unpacking by callers
            return None, None

        embedding_params = cfg.get("params", {}).copy()

        embedding_params.update(
            {
                k: v
                for k, v in cfg.items()
                if k not in ["name", "column", "model_name", "params"]
            }
        )

        self.embedding_model_wrapper = EmbeddingModelFactory.get_embedding_model(
            cfg.get("name"),
            column=cfg.get("column"),
            model_name=cfg.get("model_name"),
            #params=cfg.get("params", {}),
            **embedding_params
        )

        # Ensure embedding_model variable always exists
        embedding_model = None
        # if factory returns a wrapper exposing underlying model, unwrap it for potential use in BERTopic kwargs; otherwise return the wrapper itself (e.g., if it implements fit_transform directly)
        if hasattr(self.embedding_model_wrapper, "model") and self.embedding_model_wrapper.model is not None:
            embedding_model = self.embedding_model_wrapper.model

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
        # New implementation: use CTFIDFModelFactory if configured under model_params.ctfidf
        cfg = (self.model_params or {}).get("ctfidf")
        if not cfg:
            return None

        if not isinstance(cfg, dict):
            self.logger.warning(f"c-TF-IDF config should be a dict with 'name' and optional 'params'. Got: {cfg}")
            return None

        name = cfg.get("name") or "class_tfidf"
        params = cfg.get("params", {}) or {}

        self.logger.info(f"Building c-TF-IDF model '{name}' with params: {params}")

        # Use the new CTFIDFModelFactory
        try:
            from ctfidf_models.factory import CTFIDFModelFactory
            ctfidf_wrapper = CTFIDFModelFactory.get_ctfidf_model(name, **params)
        except Exception as e:
            self.logger.warning(f"Failed to build c-TF-IDF model via CTFIDFModelFactory: {e}")
            return None

        if ctfidf_wrapper is None:
            self.logger.warning(f"c-TF-IDF factory returned None for name '{name}'")
            return None

        # If the factory returned a wrapper with .build(), call it to get the underlying transformer
        if hasattr(ctfidf_wrapper, "build") and callable(ctfidf_wrapper.build):
            try:
                model = ctfidf_wrapper.build()
                self.logger.info(f"Called .build() on c-TF-IDF wrapper '{name}'")
                return model
            except Exception as e:
                self.logger.warning(f"Error building c-TF-IDF model '{name}': {e}")
                return None

        # Otherwise, if the wrapper exposes .model, return that
        if hasattr(ctfidf_wrapper, "model") and ctfidf_wrapper.model is not None:
            return ctfidf_wrapper.model

        # Fallback: return the wrapper itself
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

        ctfidf_model = self._build_ctfidf_model()
        if ctfidf_model is not None:
            model_kwargs["ctfidf_model"] = ctfidf_model
            model_kwargs.pop("ctfidf", None)  # Remove original config to avoid confusion
            self.logger.info(f"Added c-TF-IDF model to BERTopic kwargs: {type(ctfidf_model).__name__}")
        else:
            self.logger.warning("No c-TF-IDF model built; BERTopic will use default vectorizer behavior")

        return model_kwargs

    def run(self):
        self.logger.info(f"Running topic modelling experiment '{self.name}' with model {self.model_name}")
        #docs = self.X[self.combined_text_field_name].fillna("").tolist()

        self._log_gpu_memory("Before experiment")
        representation_docs = (
            self.X[self.representation_text_field]
            .fillna("")
            .tolist()
        )

        self.logger.info(f"Extracted {len(representation_docs)} documents for representation from field '{self.representation_text_field}'")
        #self.logger.info(f"Extracted {len(docs)} documents for topic modeling from combined text field '{self.combined_text_field_name}'")
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
            #embeddings = self.embedding_model_wrapper.transform(self.X)
            embeddings = self._get_embeddings()
            self.logger.info(f"Embedding model produced embeddings with shape {embeddings.shape}")
            embedding_hash = hashlib.sha256(
                np.ascontiguousarray(embeddings).tobytes()
            ).hexdigest()

            self.logger.info(
                "Embeddings: shape=%s, dtype=%s, hash=%s",
                embeddings.shape,
                embeddings.dtype,
                embedding_hash,
            )
        else:
            embeddings = None

        # Instantiate BERTopicModel wrapper via ModelFactory with the collected kwargs
        self.logger.info(f"Instantiating BERTopicModel with collected kwargs: {model_kwargs}")
        self.model = ModelFactory.get_model(self.model_name, **model_kwargs)
        self.logger.info(f"Instantiated BERTopicModel: {self.model}")

        # Fit/transform using docs and optional embeddings
        if embeddings is not None:
            self.logger.info("Fitting BERTopicModel with embeddings")
            topics, probs = self.model.fit_transform(representation_docs, embeddings)
        else:
            self.logger.info(f"Fitting BERTopicModel without embeddings - embeddings are generated using {self.representation_text_field} ")
            topics, probs = self.model.fit_transform(representation_docs)
        self._log_gpu_memory("After fit_transform")
        # get topic info
        # try:
        #     topic_info = self.model.get_topic_info()
        #     if topic_info is not None and self.save_path:
        #         self.logger.info(f"Saving topic info to CSV at {self.save_path}")
        #         csv_path = os.path.join(self.save_path or ".", f"{self.name}_topic_info.csv")
        #         os.makedirs(self.save_path, exist_ok=True)
        #         topic_info.to_csv(csv_path, index=False, encoding='utf-8-sig')
        #         artifacts = [csv_path]
        #     else:
        #         artifacts = []
        # except Exception as e:
        #     self.logger.warning(f"Could not get topic info: {e}")
        #     artifacts = []

        # ---------------------------------------------------------
        # Get topic information
        # ---------------------------------------------------------

        topic_info = None
        topic_hierarchy = None
        artifacts = []

        try:

            # -----------------------------------------------------
            # Topic information
            # -----------------------------------------------------

            # We always retrieve topic_info because _attach_topics()
            # uses it to attach topic metadata to the document-level
            # dataframe.
            topic_info = self.model.get_topic_info()

            if topic_info is not None:
                self.logger.info(
                    "Retrieved topic info with %d rows",
                    len(topic_info)
                )

                self.logger.info(
                    "Topic info columns: %s",
                    topic_info.columns.tolist()
                )

            # -----------------------------------------------------
            # Optional topic analysis outputs
            # -----------------------------------------------------

            if self.topic_outputs_enabled:

                self.logger.info(
                    "Topic output generation enabled"
                )

                # ---------------------------------------------
                # Hierarchical topic structure
                # ---------------------------------------------

                if self.save_topic_hierarchy:

                    topic_hierarchy = (
                        self._build_topic_hierarchy(
                            representation_docs
                        )
                    )

                    # -----------------------------------------
                    # Save topic hierarchy to configured
                    # data store
                    # -----------------------------------------

                    if topic_hierarchy is not None:

                        hierarchy_table = (
                            self._save_topic_hierarchy(
                                topic_hierarchy
                            )
                        )

                        if hierarchy_table:
                            self.logger.info(
                                "Topic hierarchy saved to SQL table: %s",
                                hierarchy_table
                            )

                        # -----------------------------------------
                        # Build hierarchy nodes
                        # -----------------------------------------

                        topic_hierarchy_nodes = (
                            self._build_topic_hierarchy_nodes(
                                topic_info=topic_info,
                                topic_hierarchy=topic_hierarchy
                            )
                        )

                        if topic_hierarchy_nodes is not None:
                            self._save_topic_hierarchy_nodes(
                                topic_hierarchy_nodes
                            )

                else:

                    self.logger.info(
                        "Topic hierarchy output disabled"
                    )

                # ---------------------------------------------
                # Save topic information
                # ---------------------------------------------

                if (
                        self.save_topic_info
                        and topic_info is not None
                ):

                    # -----------------------------------------
                    # Save topic info to configured data store
                    # -----------------------------------------

                    topic_info_table = self._save_topic_info(
                        topic_info
                    )

                    if topic_info_table:
                        self.logger.info(
                            "Topic info saved to SQL table: %s",
                            topic_info_table
                        )

                    # -----------------------------------------
                    # Optional local CSV artifact
                    # -----------------------------------------

                    if self.save_path:
                        os.makedirs(
                            self.save_path,
                            exist_ok=True
                        )

                        topic_info_path = os.path.join(
                            self.save_path,
                            f"{self.name}_topic_info.csv"
                        )

                        self.logger.info(
                            "Saving topic info to CSV: %s",
                            topic_info_path
                        )

                        topic_info.to_csv(
                            topic_info_path,
                            index=False,
                            encoding="utf-8-sig"
                        )

                        artifacts.append(
                            topic_info_path
                        )

                else:

                    if not self.save_topic_info:

                        self.logger.info(
                            "Topic info output disabled"
                        )

                    elif topic_info is None:

                        self.logger.info(
                            "No topic info available"
                        )

                # ---------------------------------------------
                # Save topic hierarchy
                # ---------------------------------------------

                if (
                        self.save_topic_hierarchy
                        and topic_hierarchy is not None
                        and self.save_path
                ):
                    os.makedirs(
                        self.save_path,
                        exist_ok=True
                    )

                    hierarchy_path = os.path.join(
                        self.save_path,
                        f"{self.name}_topic_hierarchy.csv"
                    )

                    self.logger.info(
                        "Saving topic hierarchy to CSV: %s",
                        hierarchy_path
                    )

                    topic_hierarchy.to_csv(
                        hierarchy_path,
                        index=False,
                        encoding="utf-8-sig"
                    )

                    artifacts.append(
                        hierarchy_path
                    )

                llm_topic_records = self._build_llm_topic_records(
                    topic_info=topic_info,
                    topic_hierarchy=topic_hierarchy,
                    topic_hierarchy_nodes=topic_hierarchy_nodes,
                    max_representative_docs=self.llm_max_representative_docs,
                    max_doc_characters=self.llm_max_doc_characters,
                    max_ancestors=self.llm_max_ancestors,
                )

                if llm_topic_records:
                    self.logger.info(
                        "Sample LLM topic record:\n%s",
                        json.dumps(
                            llm_topic_records[0],
                            indent=2,
                            ensure_ascii=False,
                            default=str
                        )
                    )
                # ---------------------------------------------------------
                # LLM topic processing limit
                # ---------------------------------------------------------
                llm_provider = self.llm_config.get(
                    "provider",
                    "openai"
                )

                llm_model = self.llm_config.get(
                    "model"
                )

                llm_prompt_version = self.llm_config.get(
                    "prompt_version",
                    PROMPT_VERSION
                )



                records_to_process = llm_topic_records

                if self.llm_max_topics is not None:
                    records_to_process = llm_topic_records[
                        :self.llm_max_topics
                    ]

                self.logger.info(
                    "LLM topic processing: %d of %d topics selected",
                    len(records_to_process),
                    len(llm_topic_records)
                )

                # ---------------------------------------------------------
                # Generate LLM topic themes
                # ---------------------------------------------------------

                if self.llm_config.get("enabled", False):

                    self.logger.info(
                        "Generating LLM themes for %d topics",
                        len(records_to_process)
                    )

                    llm_results = []
                    llm_failures = []

                    for topic_record in records_to_process:

                        try:
                            theme_result = self._generate_topic_theme(
                                topic_record
                            )

                            # ---------------------------------------------------------
                            # Enrich LLM result with original BERTopic metadata
                            # ---------------------------------------------------------
                            theme_result["topic_id"] = topic_record.get("topic_id")

                            theme_result["topic_label"] = topic_record.get(
                                "topic_label"
                            )

                            theme_result["topic_count"] = topic_record.get(
                                "topic_count"
                            )

                            # ---------------------------------------------------------
                            # LLM generation metadata
                            # ---------------------------------------------------------

                            llm_generated_at = datetime.now(timezone.utc).replace(tzinfo=None)

                            theme_result["llm_provider"] = llm_provider

                            theme_result["llm_model"] = llm_model

                            theme_result["prompt_version"] = llm_prompt_version

                            theme_result["generated_at"] = llm_generated_at


                            llm_results.append(
                                theme_result
                            )

                            self.logger.info(
                                "LLM theme generated for topic %s: %s",
                                topic_record.get("topic_id"),
                                theme_result
                            )
                        except Exception as e:
                            failed_topic_id = topic_record.get("topic_id")

                            self.logger.error(f"Error generating LLM theme for topic {failed_topic_id}: {e}")

                            llm_failures.append(
                                {
                                    "topic_id": failed_topic_id,
                                    "error": e,
                                }
                            )

                            continue

                        if self.llm_request_delay >0:
                            time.sleep(self.llm_request_delay)

                    llm_results_df = pd.DataFrame(
                        llm_results
                    )

                    llm_results_df = llm_results_df[
                        [
                            "topic_id",
                            "topic_label",
                            "topic_count",
                            "theme",
                            "description",
                            "confidence",
                            "llm_provider",
                            "llm_model",
                            "prompt_version",
                            "generated_at",
                        ]
                    ]

                    # ---------------------------------------------------------
                    # Save LLM result to database
                    # ---------------------------------------------------------
                    if not llm_results_df.empty:

                        llm_table = self._save_llm_results(
                            llm_results_df
                        )

                        if llm_table:
                            self.logger.info(
                                "LLM topic results saved to SQL table: %s",
                                llm_table
                            )

                    self.logger.info(
                        "Generated LLM themes for %d topics",
                        len(llm_results_df)
                    )

                    if llm_failures:
                        self.logger.warning(
                            "Failed LLM topic IDs: %s",
                            [f["topic_id"] for f in llm_failures]
                        )

                    self.logger.info(
                        "LLM theme results preview:\n%s",
                        llm_results_df.head().to_string(index=False)
                    )

                else:

                    self.logger.info(
                        "LLM topic theme generation disabled"
                    )

            else:

                self.logger.info(
                    "Topic output generation disabled"
                )

        except Exception as e:

            self.logger.exception(
                f"Could not generate topic outputs: {e}"
            )


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
            #"combined_text_field_name": self.combined_text_field_name,
            "embedding_text_field": self.model_params["embedding_model"]["column"],
            "representation_text_field": self.representation_text_field,
            "visualisations": self.visualisations,
            "topic_outputs": self.topic_outputs,
            "llm": self.llm_config,
            "topics": topics,
            "model": self.model,
        }


        #return {"df": result_df, "metadata": metadata, "artifacts": artifacts}
        return {
            "df": result_df,
            "topic_info": topic_info,
            "topic_hierarchy": topic_hierarchy,
            "metadata": metadata,
            "artifacts": artifacts
        }

    def _extract_embedding_cache_params(self):

        cfg = (
            self.model_params
            .get("embedding_model", {})
            .get("cache", {})
        )

        self.cache_enabled = cfg.get(
            "enabled",
            False
        )

        self.cache_overwrite = cfg.get(
            "overwrite",
            False
        )

        self.cache_path = Path(
            cfg.get(
                "cache_dir",
                "output/embeddings"
            )
        )

        self.embedding_id_column = cfg.get(
            "id_column",
            None
        )

    def _get_embedding_cache_base(self):

        cfg = self.model_params["embedding_model"]

        model_name = (
            cfg["model_name"]
            .replace("/", "_")
        )

        normalised_dataset_name = (
            (self.dataset_name or "unknown")
            .strip()
            .lower()
            .replace(" ", "_")
            .replace("/", "_")
        )
        chunking = cfg.get("chunking", {})

        if chunking.get("enabled", False):

            suffix = (
                f"_dataset{normalised_dataset_name}"
                f"_chunk{chunking.get('chunk_size')}"
                f"_overlap{chunking.get('overlap')}"
                f"_{chunking.get('pooling')}"
            )

        else:
            suffix = "_nochunk"

        return (
                self.cache_path /
                f"{model_name}{suffix}"
        )

    def _get_embeddings(self):

        cache_base = self._get_embedding_cache_base()

        embedding_file = cache_base.with_suffix(".npy")
        metadata_file = cache_base.with_suffix(".json")
        ids_file = cache_base.with_suffix(".csv")

        self.logger.info(f"Embedding File Cache Path: {embedding_file}")
        self.logger.info(f"Cache enabled: {self.cache_enabled}, overwrite: {self.cache_overwrite}")

        if (
                self.cache_enabled
                and embedding_file.exists()
                and not self.cache_overwrite
        ):
            self.logger.info(
                f"Loading embeddings from cache: {embedding_file}"
            )

            cached_embeddings = np.load(embedding_file)

            if len(cached_embeddings) != len(self.X):
                self.logger.warning(
                    f"Embedding cache size mismatch. "
                    f"Cache contains {len(cached_embeddings)} embeddings, "
                    f"current dataset contains {len(self.X)} documents. "
                    f"Regenerating embeddings."
                )
            else:
                return cached_embeddings

        self.logger.info(
            "Generating embeddings..."
        )

        embeddings = (
            self.embedding_model_wrapper
            .transform(self.X)
        )

        if self.cache_enabled:
            cache_base.parent.mkdir(
                parents=True,
                exist_ok=True
            )

            # 1. Save embeddings
            np.save(
                embedding_file,
                embeddings
            )

            # 2. Save document identifiers

            if not self.embedding_id_column:
                raise ValueError(
                    "embedding_model.id_column must be configured when embedding caching is enabled"
                )

            if self.embedding_id_column:

                document_ids = pd.DataFrame({
                    "row_index": range(len(self.X)),
                    "document_id": self.X[self.embedding_id_column]
                })

            else:

                document_ids = pd.DataFrame({
                    "row_index": range(len(self.X))
                })

            document_ids.to_csv(
                ids_file,
                index=False
            )

            # 3. Save metadata
            cfg = self.model_params["embedding_model"]

            metadata = {
                "model_name": cfg.get("model_name"),
                "documents": len(self.X),
                "embedding_dimensions": int(embeddings.shape[1]),
                "id_column": self.embedding_id_column,
                "chunking": cfg.get("chunking", {})
            }

            with open(
                    metadata_file,
                    "w"
            ) as f:
                json.dump(
                    metadata,
                    f,
                    indent=4
                )

            self.logger.info(
                f"Saved embedding cache: {embedding_file}"
            )

        return embeddings

    def _log_params(self):
        # Deprecated: experiments should not log directly to MLflow. Use collect_params() instead.
        return self.collect_params()

    def collect_params(self) -> dict:
        """Expose experiment params for external logging by ExperimentRunner."""
        return {
            "dataset": self.dataset_name,
            "model_name": self.model_name,
            **(self.model_params or {}),
            "evaluator_name": self.evaluator_name,
            **(self.evaluator_params or {}),
            "visualisations": self.visualisations,
            "topic_outputs": self.topic_outputs,
            "llm": self.llm_config,
            "preprocessing": self.preprocessing_metadata,
        }

    def _log_gpu_memory(self, stage):

        try:

            if torch.cuda.is_available():
                allocated = (
                        torch.cuda.memory_allocated()
                        / 1024 ** 3
                )

                reserved = (
                        torch.cuda.memory_reserved()
                        / 1024 ** 3
                )

                self.logger.info(
                    f"{stage} - "
                    f"GPU allocated: {allocated:.2f}GB, "
                    f"reserved: {reserved:.2f}GB"
                )

        except Exception as e:
            self.logger.debug(
                f"Unable to log GPU memory: {e}"
            )

    def _save_topic_hierarchy(self, hierarchy_df):
        """
        Save the BERTopic topic hierarchy using the configured
        DataSaverFactory implementation.

        The hierarchy is an auxiliary topic-analysis dataset and is
        therefore persisted separately from the document-level result.
        """

        if hierarchy_df is None or hierarchy_df.empty:
            self.logger.warning(
                "No topic hierarchy available to save"
            )
            return None

        output_cfg = self.topic_outputs.get(
            "hierarchy_output",
            {}
        )

        if not output_cfg:
            self.logger.info(
                "No hierarchy output configuration supplied; "
                "topic hierarchy will not be saved"
            )
            return None

        saver_name = output_cfg.get(
            "saver_name",
            "sql_server"
        )

        table_name = output_cfg.get(
            "table_name"
        )

        if not table_name:
            raise ValueError(
                "topic_outputs.hierarchy_output.table_name "
                "must be configured when saving topic hierarchy"
            )

        if_exists = output_cfg.get(
            "if_exists",
            "replace"
        )

        chunk_size = output_cfg.get(
            "chunk_size",
            1000
        )

        schema = output_cfg.get(
            "schema"
        )

        connector_params = output_cfg.get(
            "connector_params",
            {}
        )

        self.logger.info(
            "Saving topic hierarchy to table '%s' "
            "using saver '%s'",
            table_name,
            saver_name
        )

        try:
            from data.savers import DataSaverFactory
            from data.sqlalchemy_connector import SQLAlchemyConnector

            saver = DataSaverFactory.get_saver(
                saver_name
            )

            if saver is None:
                raise ValueError(
                    f"No saver registered with name '{saver_name}'"
                )

            connector = SQLAlchemyConnector(
                **connector_params
            )

            saver.save(
                df=hierarchy_df,
                table_name=table_name,
                connector=connector,
                if_exists=if_exists,
                chunk_size=chunk_size,
                schema=schema,
            )

            self.logger.info(
                "Topic hierarchy successfully saved to '%s'",
                table_name
            )

            return table_name

        except Exception as e:
            self.logger.exception(
                "Failed to save topic hierarchy to '%s': %s",
                table_name,
                e
            )
            raise

    def _save_topic_info(self, topic_info):
        """
        Save BERTopic topic information using the configured
        DataSaverFactory implementation.

        The topic info is the leaf-topic dataset, containing one row
        per BERTopic topic.
        """

        if topic_info is None or topic_info.empty:
            self.logger.warning(
                "No topic info available to save"
            )
            return None


        output_cfg = self.topic_outputs.get(
            "topic_info_output",
            {}
        )

        if not output_cfg:
            self.logger.info(
                "No topic info output configuration supplied; "
                "topic info will not be saved"
            )
            return None

        saver_name = output_cfg.get(
            "saver_name",
            "sql_server"
        )

        table_name = output_cfg.get(
            "table_name"
        )

        if not table_name:
            raise ValueError(
                "topic_outputs.topic_info_output.table_name "
                "must be configured when saving topic info"
            )

        if_exists = output_cfg.get(
            "if_exists",
            "replace"
        )

        chunk_size = output_cfg.get(
            "chunk_size",
            1000
        )

        schema = output_cfg.get(
            "schema"
        )

        connector_params = output_cfg.get(
            "connector_params",
            {}
        )

        self.logger.info(
            "Saving topic info to table '%s' "
            "using saver '%s'",
            table_name,
            saver_name
        )

        try:
            from data.savers import DataSaverFactory
            from data.sqlalchemy_connector import SQLAlchemyConnector

            saver = DataSaverFactory.get_saver(
                saver_name
            )

            if saver is None:
                raise ValueError(
                    f"No saver registered with name '{saver_name}'"
                )

            connector = SQLAlchemyConnector(
                **connector_params
            )

            topic_info_sql = self._prepare_topic_info_for_sql(
                topic_info
            )

            saver.save(
                df=topic_info_sql,
                table_name=table_name,
                connector=connector,
                if_exists=if_exists,
                chunk_size=chunk_size,
                schema=schema,
            )

            self.logger.info(
                "Topic info successfully saved to '%s'",
                table_name
            )

            return table_name

        except Exception as e:
            self.logger.exception(
                "Failed to save topic info to '%s': %s",
                table_name,
                e
            )
            raise

    def _prepare_topic_info_for_sql(self, topic_info):
        """
        Prepare BERTopic topic_info for relational storage.

        BERTopic stores Representation and Representative_Docs as
        Python lists. These are serialised as JSON strings so they
        can be stored in SQL Server while retaining their structure.
        """

        topic_info_sql = topic_info.copy()

        list_columns = [
            "Representation",
            "Representative_Docs",
        ]

        for column in list_columns:

            if column not in topic_info_sql.columns:
                continue

            topic_info_sql[column] = topic_info_sql[column].apply(
                lambda value: json.dumps(
                    value,
                    ensure_ascii=False
                )
                if isinstance(value, (list, tuple))
                else value
            )

        return topic_info_sql

    def _save_topic_hierarchy_nodes(self, topic_hierarchy_nodes):
        """
        Save the BERTopic topic hierarchy using the configured
        DataSaverFactory implementation.

        The hierarchy is an auxiliary topic-analysis dataset and is
        therefore persisted separately from the document-level result.
        """

        if topic_hierarchy_nodes is None or topic_hierarchy_nodes.empty:
            self.logger.warning(
                "No topic hierarchy nodes available to save"
            )
            return None

        output_cfg = self.topic_outputs.get(
            "hierarchy_nodes_output",
            {}
        )

        if not output_cfg:
            self.logger.info(
                "No hierarchy nodes output configuration supplied; "
                "topic hierarchy nodes will not be saved"
            )
            return None

        saver_name = output_cfg.get(
            "saver_name",
            "sql_server"
        )

        table_name = output_cfg.get(
            "table_name"
        )

        if not table_name:
            raise ValueError(
                "topic_outputs.hierarchy_nodes_output.table_name "
                "must be configured when saving topic hierarchy nodes"
            )

        if_exists = output_cfg.get(
            "if_exists",
            "replace"
        )

        chunk_size = output_cfg.get(
            "chunk_size",
            1000
        )

        schema = output_cfg.get(
            "schema"
        )

        connector_params = output_cfg.get(
            "connector_params",
            {}
        )

        self.logger.info(
            "Saving topic hierarchy nodes to table '%s' "
            "using saver '%s'",
            table_name,
            saver_name
        )

        try:
            from data.savers import DataSaverFactory
            from data.sqlalchemy_connector import SQLAlchemyConnector

            saver = DataSaverFactory.get_saver(
                saver_name
            )

            if saver is None:
                raise ValueError(
                    f"No saver registered with name '{saver_name}'"
                )

            connector = SQLAlchemyConnector(
                **connector_params
            )

            topic_hierarchy_nodes_sql = (
                self._prepare_topic_hierarchy_nodes_for_sql(
                    topic_hierarchy_nodes
                )
            )

            saver.save(
                df=topic_hierarchy_nodes_sql,
                table_name=table_name,
                connector=connector,
                if_exists=if_exists,
                chunk_size=chunk_size,
                schema=schema,
            )

            self.logger.info(
                "Topic hierarchy nodes successfully saved to '%s'",
                table_name
            )

            return table_name

        except Exception as e:
            self.logger.exception(
                "Failed to save topic hierarchy nodes to '%s': %s",
                table_name,
                e
            )
            raise

    def _prepare_topic_hierarchy_nodes_for_sql(self, topic_hierarchy_nodes):
        """
        Prepare topic hierarchy nodes for relational storage.

        BERTopic hierarchy nodes may contain Python lists for fields such as
        top_words and representative_docs. These are serialised as JSON strings
        so they can be stored in SQL Server while retaining their structure.
        """

        nodes_sql = topic_hierarchy_nodes.copy()

        list_columns = [
            "top_words",
            "representative_docs",
        ]

        for column in list_columns:

            if column not in nodes_sql.columns:
                continue

            nodes_sql[column] = nodes_sql[column].apply(
                lambda value: json.dumps(
                    value,
                    ensure_ascii=False
                )
                if isinstance(value, (list, tuple))
                else value
            )

        return nodes_sql

    def _build_llm_topic_records(
            self,
            topic_info,
            topic_hierarchy,
            topic_hierarchy_nodes,
            max_representative_docs=3,
            max_doc_characters=500,
            max_ancestors=3,
    ):
        """
        Build structured, LLM-ready records for each leaf topic.

        Each record combines:
            - BERTopic topic information
            - a limited number of representative documents
            - an immediate parent
            - sibling topics
            - a limited number of higher-level ancestors

        No LLM calls are made here. This method only prepares the
        contextual information that will later be supplied to an LLM.

        Parameters
        ----------
        topic_info : pd.DataFrame
            BERTopic topic information.

        topic_hierarchy : pd.DataFrame
            BERTopic hierarchical merge information.

        topic_hierarchy_nodes : pd.DataFrame
            Relational representation of all leaf and parent nodes.

        max_representative_docs : int
            Maximum number of representative documents retained per topic.

        max_doc_characters : int
            Maximum number of characters retained from each representative
            document.

        max_ancestors : int
            Maximum number of higher-level ancestors retained.
        """

        self.logger.info(
            "Building LLM-ready topic records"
        )

        if topic_info is None or topic_info.empty:
            self.logger.warning(
                "Cannot build LLM topic records: topic_info is empty"
            )
            return []

        if (
                topic_hierarchy_nodes is None
                or topic_hierarchy_nodes.empty
        ):
            self.logger.warning(
                "Cannot build LLM topic records: "
                "topic_hierarchy_nodes is empty"
            )
            return []

        self.logger.info(
            "LLM record configuration: "
            "max_representative_docs=%d, "
            "max_doc_characters=%d, "
            "max_ancestors=%d",
            max_representative_docs,
            max_doc_characters,
            max_ancestors,
        )

        # ---------------------------------------------------------
        # Identify leaf topics
        # ---------------------------------------------------------

        leaf_nodes = topic_hierarchy_nodes[
            topic_hierarchy_nodes["node_type"] == "leaf"
            ].copy()

        # Exclude BERTopic's outlier topic (-1).
        leaf_nodes = leaf_nodes[
            leaf_nodes["node_id"] != -1
            ].copy()

        self.logger.info(
            "Preparing LLM records for %d leaf topics",
            len(leaf_nodes)
        )

        # ---------------------------------------------------------
        # Build node lookup
        # ---------------------------------------------------------

        nodes_by_id = (
            topic_hierarchy_nodes
            .set_index("node_id")
            .to_dict("index")
        )

        # ---------------------------------------------------------
        # Build topic_info lookup
        # ---------------------------------------------------------

        topic_info_by_id = (
            topic_info
            .set_index("Topic")
            .to_dict("index")
        )

        records = []

        # ---------------------------------------------------------
        # Process each leaf topic
        # ---------------------------------------------------------

        for _, leaf in leaf_nodes.iterrows():

            topic_id = int(leaf["node_id"])

            topic_info_row = topic_info_by_id.get(
                topic_id,
                {}
            )

            # -----------------------------------------------------
            # Representative documents
            # -----------------------------------------------------

            representative_docs = topic_info_row.get(
                "Representative_Docs",
                leaf.get("representative_docs")
            )

            if representative_docs is None:
                representative_docs = []

            # Ensure we are working with a list
            if not isinstance(
                    representative_docs,
                    (list, tuple)
            ):
                representative_docs = [
                    representative_docs
                ]

            representative_docs = list(
                representative_docs
            )[:max_representative_docs]

            # Truncate individual documents
            truncated_docs = []

            for document in representative_docs:

                if document is None:
                    continue

                document = str(document).strip()

                if len(document) > max_doc_characters:
                    document = (
                            document[:max_doc_characters].rstrip()
                            + " ..."
                    )

                truncated_docs.append(
                    document
                )

            # -----------------------------------------------------
            # Basic topic information
            # -----------------------------------------------------

            record = {
                "topic_id": topic_id,

                "topic_label": topic_info_row.get(
                    "Name",
                    leaf.get("topic_label")
                ),

                "topic_count": topic_info_row.get(
                    "Count",
                    leaf.get("topic_count")
                ),

                "top_words": topic_info_row.get(
                    "Representation",
                    leaf.get("top_words")
                ),

                "representative_docs": truncated_docs,

                "parent": None,

                "siblings": [],

                "ancestors": [],
            }

            # -----------------------------------------------------
            # Find immediate parent
            # -----------------------------------------------------

            parent_id = leaf.get("parent_id")

            if pd.notna(parent_id):

                parent_id = int(parent_id)

                parent_node = nodes_by_id.get(
                    parent_id
                )

                if parent_node is not None:
                    record["parent"] = {
                        "node_id": parent_id,
                        "label": parent_node.get(
                            "topic_label"
                        ),
                    }

            # -----------------------------------------------------
            # Find siblings
            # -----------------------------------------------------

            if (
                    parent_id is not None
                    and pd.notna(parent_id)
            ):

                parent_node = nodes_by_id.get(
                    parent_id
                )

                if parent_node is not None:

                    child_ids = [
                        parent_node.get(
                            "child_left_id"
                        ),
                        parent_node.get(
                            "child_right_id"
                        ),
                    ]

                    for child_id in child_ids:

                        if pd.isna(child_id):
                            continue

                        child_id = int(child_id)

                        # Don't include current topic
                        if child_id == topic_id:
                            continue

                        sibling_node = nodes_by_id.get(
                            child_id
                        )

                        if sibling_node is not None:
                            record["siblings"].append(
                                {
                                    "node_id": child_id,
                                    "label": sibling_node.get(
                                        "topic_label"
                                    ),
                                }
                            )

            # -----------------------------------------------------
            # Traverse higher-level ancestors
            # -----------------------------------------------------

            current_parent_id = parent_id

            visited = set()

            while (
                    current_parent_id is not None
                    and pd.notna(current_parent_id)
                    and len(record["ancestors"]) < max_ancestors
            ):

                current_parent_id = int(
                    current_parent_id
                )

                # Protect against malformed/cyclic hierarchy
                if current_parent_id in visited:
                    self.logger.warning(
                        "Cycle detected while traversing "
                        "hierarchy for topic %s",
                        topic_id
                    )

                    break

                visited.add(
                    current_parent_id
                )

                parent_node = nodes_by_id.get(
                    current_parent_id
                )

                if parent_node is None:
                    break

                # Skip immediate parent because it is already
                # represented separately in record["parent"].
                if current_parent_id != parent_id:
                    record["ancestors"].append(
                        {
                            "node_id": current_parent_id,
                            "label": parent_node.get(
                                "topic_label"
                            ),
                        }
                    )

                next_parent_id = parent_node.get(
                    "parent_id"
                )

                if (
                        next_parent_id is None
                        or pd.isna(next_parent_id)
                ):
                    break

                current_parent_id = next_parent_id

            records.append(
                record
            )

        self.logger.info(
            "Generated %d LLM-ready topic records",
            len(records)
        )

        return records

    # def _build_topic_theme_prompt(self, topic_record):
    #     """
    #     Build the prompt used to generate a human-readable theme for
    #     a single BERTopic topic.
    #
    #     The topic record contains the leaf topic information together
    #     with its hierarchical context.
    #     """
    #
    #     topic_id = topic_record.get("topic_id")
    #     topic_label = topic_record.get("topic_label")
    #     topic_count = topic_record.get("topic_count")
    #     top_words = topic_record.get("top_words", [])
    #     representative_docs = topic_record.get(
    #         "representative_docs",
    #         []
    #     )
    #
    #     parent = topic_record.get("parent")
    #     siblings = topic_record.get("siblings", [])
    #     ancestors = topic_record.get("ancestors", [])
    #
    #     parent_label = (
    #         parent.get("label")
    #         if isinstance(parent, dict)
    #         else None
    #     )
    #
    #     sibling_labels = [
    #         sibling.get("label")
    #         for sibling in siblings
    #         if isinstance(sibling, dict)
    #     ]
    #
    #     ancestor_labels = [
    #         ancestor.get("label")
    #         for ancestor in ancestors
    #         if isinstance(ancestor, dict)
    #     ]
    #
    #     prompt = f"""
    # You are analysing topics generated from an Irish parliamentary
    # question-and-answer corpus.
    #
    # Your task is to assign a concise, meaningful human-readable theme
    # to the topic below.
    #
    # The theme should describe the substantive subject matter of the
    # topic rather than simply repeating its most frequent words.
    #
    # Use the representative documents as the primary evidence.
    # Use the top words and hierarchical context as supporting evidence.
    #
    # Do not assume that the automatically generated BERTopic labels are
    # semantically correct. They are only clues.
    #
    # TOPIC
    # -----
    #
    # Topic ID:
    # {topic_id}
    #
    # Topic label:
    # {topic_label}
    #
    # Number of documents:
    # {topic_count}
    #
    # Top words:
    # {top_words}
    #
    # Representative documents:
    # {representative_docs}
    #
    # HIERARCHICAL CONTEXT
    # --------------------
    #
    # Parent topic:
    # {parent_label}
    #
    # Sibling topics:
    # {sibling_labels}
    #
    # Ancestor topics:
    # {ancestor_labels}
    #
    # TASK
    # ----
    #
    # Determine the main substantive theme of this topic.
    #
    # Return ONLY valid JSON using exactly this structure:
    #
    # {{
    #   "topic_id": {topic_id},
    #   "theme": "A concise human-readable theme",
    #   "description": "A one or two sentence description explaining what this topic concerns.",
    #   "confidence": 0.0
    # }}
    #
    # Requirements:
    #
    # - The theme should normally be between 3 and 10 words.
    # - The description should identify the main subject matter represented
    #   by the topic.
    # - Base the interpretation primarily on the representative documents.
    # - Use the hierarchical information to help disambiguate the topic.
    # - Do not simply reproduce the topic label.
    # - Do not mention BERTopic, clustering, embeddings or this prompt.
    # - Confidence must be a number between 0.0 and 1.0.
    # """
    #
    #     return prompt.strip()

    def _generate_topic_theme(self, topic_record):
        """
        Generate an LLM-derived theme for a single topic record.

        This method deliberately processes one topic at a time.
        Batch processing will be added once the prompt and output
        have been validated.
        """

        if not topic_record:
            raise ValueError(
                "Cannot generate topic theme from an empty topic record"
            )

        prompt = build_topic_theme_prompt(
            topic_record
        )

        llm_config = self.llm_config

        if not llm_config:
            raise ValueError(
                "No LLM configuration found in model_params.llm"
            )

        provider = llm_config.get(
            "provider",
            "openai"
        )

        model = llm_config.get(
            "model"
        )

        if not model:
            raise ValueError(
                "LLM model must be configured in model_params.llm.model"
            )

        self.logger.info(
            "Generating LLM theme for topic %s using %s/%s",
            topic_record.get("topic_id"),
            provider,
            model
        )

        try:

            from llm.factory import LLMFactory

            llm = LLMFactory.create(
                provider=provider,
                model=model,
                max_retries=self.llm_max_retries,
                retry_delay=self.llm_retry_delay
            )

            response = llm.generate(
                prompt
            )

            self.logger.info(
                "Generated LLM theme for topic %s",
                topic_record.get("topic_id")
            )

            try:
                result = json.loads(response)
            except json.JSONDecodeError as e:
                self.logger.error(
                    "Invalid JSON returned by LLM for topic %s: %s",
                    topic_record.get("topic_id"),
                    response
                )
                raise ValueError(
                    f"LLM returned invalid JSON for topic "
                    f"{topic_record.get('topic_id')}"
                ) from e

            required_fields = [
                "topic_id",
                "theme",
                "description",
                "confidence",
            ]

            missing = [
                field
                for field in required_fields
                if field not in result
            ]

            if missing:
                raise ValueError(
                    f"LLM response for topic {topic_record.get('topic_id')} "
                    f"is missing fields: {missing}"
                )

            return result

        except Exception as e:

            self.logger.exception(
                "Failed to generate LLM theme for topic %s: %s",
                topic_record.get("topic_id"),
                e
            )

            raise

    def _save_llm_results(self, llm_results_df):
        """
        Save LLM-generated topic themes using the configured
        DataSaverFactory implementation.
        """

        if llm_results_df is None or llm_results_df.empty:
            self.logger.warning(
                "No LLM topic results available to save"
            )
            return None

        output_cfg = self.llm_config.get(
            "llm_output",
            {}
        )

        if not output_cfg:
            self.logger.info(
                "No LLM output configuration supplied; "
                "LLM results will not be saved"
            )
            return None

        saver_name = output_cfg.get(
            "saver_name",
            "sql_server"
        )

        table_name = output_cfg.get(
            "table_name"
        )

        if not table_name:
            raise ValueError(
                "llm.llm_output.table_name must be configured "
                "when saving LLM results"
            )

        if_exists = output_cfg.get(
            "if_exists",
            "replace"
        )

        chunk_size = output_cfg.get(
            "chunk_size",
            1000
        )

        schema = output_cfg.get(
            "schema"
        )

        connector_params = output_cfg.get(
            "connector_params",
            {}
        )

        self.logger.info(
            "Saving LLM topic results to table '%s' "
            "using saver '%s'",
            table_name,
            saver_name
        )

        try:

            from data.savers import DataSaverFactory
            from data.sqlalchemy_connector import SQLAlchemyConnector

            saver = DataSaverFactory.get_saver(
                saver_name
            )

            if saver is None:
                raise ValueError(
                    f"No saver registered with name '{saver_name}'"
                )

            connector = SQLAlchemyConnector(
                **connector_params
            )

            saver.save(
                df=llm_results_df,
                table_name=table_name,
                connector=connector,
                if_exists=if_exists,
                chunk_size=chunk_size,
                schema=schema,
            )

            self.logger.info(
                "LLM topic results successfully saved to '%s'",
                table_name
            )

            return table_name

        except Exception as e:

            self.logger.exception(
                "Failed to save LLM topic results to '%s': %s",
                table_name,
                e
            )

            raise