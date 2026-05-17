# models/bertopic_model.py
from .base import Model
from logs.logger import get_logger
from typing import Any, Dict
from bertopic import BERTopic


class BERTopicModel(Model):
    """Wrapper around BERTopic to fit into ModelFactory pattern.

    This wrapper extracts commonly-used subcomponents from the provided params and
    constructs the internal BERTopic instance with those components. The experiment is
    responsible for creating and passing embeddings to fit/transform; embedding_model is
    therefore not passed to the BERTopic constructor even if present in params.
    """
    def __init__(self, name: str = "bertopic", **params: Dict[str, Any]):
        super().__init__(name, **params)
        self.logger = get_logger(self.__class__.__name__)
        # Preserve original param dict for metadata, but build a cleaned set for BERTopic init
        self.params = params or {}
        # Build kwargs to pass to BERTopic constructor. Start from a shallow copy.
        model_init_kwargs = dict(self.params or {})

        # embedding_model should not be passed to BERTopic constructor (embeddings passed to fit/transform)
        model_init_kwargs.pop("embedding_model", None)

        self.logger.info(f"Model Init Kwargs before mapping: {model_init_kwargs}")

        # Support both explicit BERTopic kwargs and our higher-level keys coming from experiment layer.
        # Map common high-level keys to BERTopic constructor arg names if needed.
        if "dimensionality_reduction_model" in model_init_kwargs and "umap_model" not in model_init_kwargs:
            model_init_kwargs["umap_model"] = model_init_kwargs.pop("dimensionality_reduction_model")

        self.logger.info(f"Preparing to extract clusterer params")
        if "clusterer" in model_init_kwargs and "hdbscan_model" not in model_init_kwargs:
            self.logger.info("Mapping 'clusterer' param to 'hdbscan_model' for BERTopic constructor")
            model_init_kwargs["hdbscan_model"] = model_init_kwargs.pop("clusterer")
        else:
            self.logger.info("No 'clusterer' param found or 'hdbscan_model' already present; skipping clusterer mapping")

        self.logger.info(f"Preparing to extract vectorizer params")
        if "vectorizer" in model_init_kwargs and "vectorizer_model" not in model_init_kwargs:
            self.logger.info(f"Mapping 'vectorizer' param to 'vectorizer_model' for BERTopic constructor")
            model_init_kwargs["vectorizer_model"] = model_init_kwargs.pop("vectorizer")

        # ctfidf may be provided under 'ctfidf' or 'ctfidf_model' - forward as-is if present
        if "ctfidf" in model_init_kwargs and "ctfidf_model" not in model_init_kwargs:
            model_init_kwargs["ctfidf_model"] = model_init_kwargs.pop("ctfidf")

        # Store the prepared kwargs for later use when building the internal model
        self._model_init_kwargs = model_init_kwargs

        # Attempt to construct the internal BERTopic instance immediately. If building fails (e.g.,
        # missing optional dependencies), log a warning and defer construction until fit/transform.
        self.model = None
        self.topics_ = None
        self.probs_ = None
        try:
            self.build()
        except Exception as e:
            # Do not raise here; allow deferred build in fit/transform
            self.logger.warning(f"BERTopic build deferred due to error during initialization: {e}")

    def build(self):
        self.logger.info(f"Building BERTopicModel with parameters: {self._model_init_kwargs}")
        # Construct BERTopic with the prepared kwargs; BERTopic will use defaults for any omitted args
        self.model = BERTopic(**self._model_init_kwargs)
        return self.model

    def fit(self, X, embeddings=None, y=None):
        """Fit BERTopic model on documents X, optionally using embeddings."""
        self.logger.info("Fitting BERTopicModel")
        if self.model is None:
            self.build()
        if embeddings is not None:
            self.logger.info("Using provided embeddings for fit")
            self.topics_, self.probs_ = self.model.fit_transform(X, embeddings)
        else:
            self.logger.info("No embeddings provided; fitting BERTopic with default embedding model")
            self.topics_, self.probs_ = self.model.fit_transform(X)
        return self

    def transform(self, X, embeddings=None):
        self.logger.info("Transforming data with BERTopicModel")
        if self.model is None:
            self.logger.error("Attempted to transform with unbuilt/unfitted model")
            raise RuntimeError("Model not built/fitted")

        if embeddings is not None:
            return self.model.transform(X, embeddings)

        return self.model.transform(X)

    def fit_transform(self, X, embeddings=None):
        self.logger.info("Starting fit_transform for BERTopicModel")
        if self.model is None:
            self.build()

        if embeddings is not None:
            self.logger.info("Using provided embeddings for fit_transform")
            return self.model.fit_transform(X, embeddings)
        else:
            return self.model.fit_transform(X)

    def get_topic_info(self):
        if self.model is None:
            self.logger.error("Attempted to get topic info from unbuilt/unfitted model")
        return self.model.get_topic_info()

    def get_topic(self, topic_id):
        if self.model is None:
            self.logger.error("Attempted to get topic from unbuilt/unfitted model")
        return self.model.get_topic(topic_id)

    def get_topics(self):
        if self.model is None:
            self.logger.error("Attempted to get topics from unbuilt/unfitted model")
        return self.model.get_topics()
