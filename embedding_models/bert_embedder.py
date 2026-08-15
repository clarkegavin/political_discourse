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

        device = self.params.get(
            "device",
            "cpu"
        )
        self.logger.info(f"Initializing BERTEmbeddingModel with device: {device}")
        try:
            self.model = SentenceTransformer(model_name, device=device)
            self.tokenizer = self.model.tokenizer

            self.logger.info(
                f"Loaded BERT model '{model_name}' successfully."
            )
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
            self.model = SentenceTransformer(modules=[word_embedding_model, pooling_model], device=device)
            self.tokenizer = self.model.tokenizer
            self.logger.info(f"Initialized fallback BERT embedding model with pooling strategy '{pooling_strategy}'.")
        self._extract_chunking_params()



    def fit(self, X):
        return  # no fitting

    def transform(self, X):
        self.logger.info(f"Transforming data using BERTEmbeddingModel {self.model_name} on column '{self.column}'")
        texts = X[self.column].fillna("").tolist()

        # Prefix logic - works for E5 models, which can benefit from task-specific prefixes, but is optional and can be used with any model
        prefix = self.params.get("prefix")
        if prefix:
            texts = [f"{prefix}: {t}" for t in texts]

        if self.chunking_enabled:
            self.logger.info(f"Chunking enabled: chunk_size={self.chunk_size}, overlap={self.chunk_overlap}, pooling={self.chunk_pooling}")
            embeddings = self.encode_documents(texts)

        else:
            embeddings = self.model.encode(
                texts,
                **self._encode_params()
            )

        self._dim = embeddings.shape[1]

        return embeddings
        #return self.model.encode(X[self.column].tolist(), show_progress_bar=False)

    def get_feature_names(self):
        """Return placeholder feature names for pipeline compatibility."""
        if self._dim is None:
            return []  # not fit yet
        return np.array([f"bert_dim_{i}" for i in range(self._dim)])

    @property
    def bertopic_model(self):
        """Return a BERTopic-compatible embedding model wrapper."""
        return self.model

    def _extract_chunking_params(self):
        """Extract chunking parameters from self.params."""
        self.chunking_config = self.params.get("chunking", {})

        self.chunking_enabled = self.chunking_config.get(
            "enabled",
            False
        )


        self.chunk_overlap = self.chunking_config.get(
            "overlap",
            32
        )

        self.logger.info(f"Chunking enabled: {self.chunking_enabled}, overlap: {self.chunk_overlap}")
        self.logger.info(f"Model {self.model_name} max sequence length: {self.model.max_seq_length}")
        self.chunk_size = self.chunking_config.get(
            "chunk_size",
            self.model.max_seq_length - self.chunk_overlap -2
        )

        self.logger.info(f"Chunk size set to: {self.chunk_size}")

        if self.chunk_overlap >= self.chunk_size:
            raise ValueError(
                "Chunk overlap must be smaller than chunk size"
            )

        self.chunk_pooling = self.chunking_config.get(
            "pooling",
            "mean"
        )

    def encode_documents(self, texts):

        normal_docs = []
        chunk_docs = []

        for idx, text in enumerate(texts):

            token_count = len(
                self.tokenizer.encode(
                    text,
                    add_special_tokens=False,
                    truncation=False
                )
            )

            if token_count <= self.model.max_seq_length - 2:
                normal_docs.append((idx, text))

            else:
                chunk_docs.append((idx, text))

        self.logger.info(
            f"Documents requiring chunking: {len(chunk_docs)} / {len(texts)}"
        )

        embeddings = [None] * len(texts)

        #
        # Encode normal documents
        #
        if normal_docs:

            normal_embeddings = self.model.encode(
                [text for _, text in normal_docs],
                **self._encode_params()
            )

            for (idx, _), embedding in zip(
                    normal_docs,
                    normal_embeddings
            ):
                embeddings[idx] = embedding

        #
        # Create ALL chunks from ALL long documents
        #
        all_chunks = []
        chunk_mapping = []

        for idx, text in chunk_docs:

            chunks = self.create_chunks(text)

            for chunk in chunks:
                all_chunks.append(chunk)
                chunk_mapping.append(idx)

        self.logger.info(
            f"Created {len(all_chunks)} chunks from "
            f"{len(chunk_docs)} documents"
        )

        #
        # Single batched GPU encoding
        #
        if all_chunks:


            chunk_embeddings = self.model.encode(
                all_chunks,
                **self._encode_params()
            )

            #
            # Pool chunks back to documents
            #
            document_chunks = {}

            for doc_idx, embedding in zip(
                    chunk_mapping,
                    chunk_embeddings
            ):

                if doc_idx not in document_chunks:
                    document_chunks[doc_idx] = []

                document_chunks[doc_idx].append(
                    embedding
                )

            for doc_idx, chunks in document_chunks.items():

                chunk_array = np.vstack(chunks)

                if self.chunk_pooling == "mean":
                    embeddings[doc_idx] = np.mean(
                        chunk_array,
                        axis=0
                    )

                elif self.chunk_pooling == "max":
                    embeddings[doc_idx] = np.max(
                        chunk_array,
                        axis=0
                    )

                else:
                    raise ValueError(
                        f"Unsupported pooling method: {self.chunk_pooling}"
                    )

        return np.vstack(embeddings)

    def create_chunks(self, text):

        tokens = self.tokenizer.encode(
            text,
            add_special_tokens=False
        )

        chunks = []

        step = self.chunk_size - self.chunk_overlap

        for start in range(0, len(tokens), step):

            chunk_tokens = tokens[
                start:start + self.chunk_size - 2 # as model will add [CLS], [SEP]
            ]

            chunk_text = self.tokenizer.decode(
                chunk_tokens,
                skip_special_tokens=True,
                clean_up_tokenization_spaces=False
            )

            # Safety check after decode/re-tokenisation
            encoded_length = len(
                self.tokenizer.encode(
                    chunk_text,
                    add_special_tokens=False
                )
            )

            if encoded_length > self.chunk_size:
                chunk_text = self.tokenizer.decode(
                    chunk_tokens[:self.chunk_size - 10],
                    skip_special_tokens=True,
                    clean_up_tokenization_spaces=False
                )


            chunks.append(chunk_text)

        return chunks

    def _encode_params(self):
        return {
            "batch_size": self.params.get("batch_size", 32),
            "show_progress_bar": False,
            "convert_to_numpy": True
        }