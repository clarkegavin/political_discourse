# embedding_models/bert_embedder.py
from sentence_transformers import SentenceTransformer, models
from .base import EmbeddingModel
import numpy as np
from logs.logger import get_logger

class BERTEmbeddingModel(EmbeddingModel):
    def __init__(self, name: str, column: str, model_name="sentence-transformers/all-mpnet-base-v2", **params):
        self.name = name
        self.column = column
        # self.model = SentenceTransformer(model_name)
        self.model_name = model_name
        self.logger = get_logger(self.__class__.__name__)
        self._dim = None  # to be set after fitting
        self.params = params

        try:
            self.model = SentenceTransformer(model_name)
            self.logger.info(f"Loaded BERT model '{model_name}' successfully.")
        except Exception as e:
            self.logger.warning(f"Failed to load SentenceTransformer model '{model_name}'. Falling back to Transformer + Pooling. Error: {e}")

            # fallback: wrap hugging face model with pooling to get sentence embeddings
            word_embedding_model = models.Transformer(model_name)
            pooling_strategy = self.params.get("pooling_strategy", "mean")
            pooling_model = models.Pooling(
                word_embedding_model.get_word_embedding_dimension(),
                pooling_mode_mean_tokens=(pooling_strategy == "mean"),
                pooling_mode_cls_token=(pooling_strategy == "cls"),
                pooling_mode_max_tokens=(pooling_strategy == "max"),
            )
            self.model = SentenceTransformer(modules=[word_embedding_model, pooling_model])
            self.logger.info(f"Initialized fallback BERT embedding model with pooling strategy '{pooling_strategy}'.")



    def fit(self, X):
        return  # no fitting

    def transform(self, X):
        self.logger.info(f"Transforming data using BERTEmbeddingModel {self.model_name} on column '{self.column}'")
        texts = X[self.column].fillna("").tolist()

        # Prefix logic - works for E5 models, which can benefit from task-specific prefixes, but is optional and can be used with any model
        prefix = self.params.get("prefix")
        if prefix:
            texts = [f"{prefix}: {t}" for t in texts]

        embeddings = self.model.encode(texts,
                                       batch_size = self.params.get("batch_size", 32),
                                       show_progress_bar=False)

        self._dim = embeddings.shape[1]

        return embeddings
        #return self.model.encode(X[self.column].tolist(), show_progress_bar=False)

    def get_feature_names(self):
        """Return placeholder feature names for pipeline compatibility."""
        if self._dim is None:
            return []  # not fit yet
        return np.array([f"bert_dim_{i}" for i in range(self._dim)])