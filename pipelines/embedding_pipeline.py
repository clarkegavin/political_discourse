import json
from pathlib import Path

import numpy as np
import pandas as pd

from .base import Pipeline
from logs.logger import get_logger
from embedding_models.factory import EmbeddingModelFactory
import hashlib

class EmbeddingPipeline(Pipeline):

    def __init__(
        self,
        sort_columns=None,
        text_field="TopicEmbeddingText",
        dataset_name="unknown",
        embedding_model=None,
        **kwargs
    ):

        super().__init__(
            name=kwargs.get(
                "name",
                self.__class__.__name__
            )
        )

        self.logger = get_logger(
            self.__class__.__name__
        )

        self.sort_columns = sort_columns or []
        self.text_field = text_field
        self.dataset_name = dataset_name
        self.model_params = {
            "embedding_model": embedding_model or {}
        }

        self.embedding_model_wrapper = None

        self._extract_embedding_cache_params()
        self._build_embedding_model()

        self.logger.info(
            f"Initialized EmbeddingPipeline "
            f"with text_field={self.text_field}, "
            f"dataset_name={self.dataset_name}"
        )

    # --------------------------------------------------
    # Model
    # --------------------------------------------------

    def _build_embedding_model(self):

        cfg = self.model_params["embedding_model"]

        if not cfg:
            raise ValueError(
                "embedding_model configuration "
                "must be provided."
            )

        embedding_params = (
            cfg.get("params", {})
            .copy()
        )

        embedding_params.update(
            {
                k: v
                for k, v in cfg.items()
                if k not in [
                    "name",
                    "column",
                    "model_name",
                    "params",
                    "cache",
                ]
            }
        )

        self.embedding_model_wrapper = (
            EmbeddingModelFactory
            .get_embedding_model(
                cfg.get("name"),
                column=cfg.get("column"),
                model_name=cfg.get("model_name"),
                **embedding_params
            )
        )

        self.logger.info(
            f"Built embedding model: "
            f"{self.embedding_model_wrapper}"
        )

    # --------------------------------------------------
    # Cache configuration
    # --------------------------------------------------

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
            "id_column"
        )

    # --------------------------------------------------
    # Cache path
    # --------------------------------------------------

    def _get_embedding_cache_base(self):

        cfg = self.model_params[
            "embedding_model"
        ]

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

        chunking = cfg.get(
            "chunking",
            {}
        )

        if chunking.get(
            "enabled",
            False
        ):

            suffix = (
                f"_dataset{normalised_dataset_name}"
                f"_chunk{chunking.get('chunk_size')}"
                f"_overlap{chunking.get('overlap')}"
                f"_{chunking.get('pooling')}"
            )

        else:

            suffix = (
                f"_dataset{normalised_dataset_name}"
                "_nochunk"
            )

        return (
            self.cache_path /
            f"{model_name}{suffix}"
        )

    # --------------------------------------------------
    # Embeddings
    # --------------------------------------------------

    def _get_embeddings(self, data):

        cache_base = self._get_embedding_cache_base()

        embedding_file = cache_base.with_suffix(".npy")
        metadata_file = cache_base.with_suffix(".json")
        ids_file = cache_base.with_suffix(".csv")

        self.logger.info(
            f"Embedding File Cache Path: {embedding_file}"
        )

        # --------------------------------------------------
        # Validate cache configuration
        # --------------------------------------------------

        if (
                self.cache_enabled
                and not self.embedding_id_column
        ):
            raise ValueError(
                "embedding_model.cache.id_column "
                "must be configured when embedding "
                "caching is enabled."
            )

        # --------------------------------------------------
        # Try existing cache
        # --------------------------------------------------

        if (
                self.cache_enabled
                and embedding_file.exists()
                and ids_file.exists()
                and not self.cache_overwrite
        ):

            self.logger.info(
                f"Loading embeddings from cache: "
                f"{embedding_file}"
            )

            cached_embeddings = np.load(
                embedding_file
            )

            cached_ids = (
                pd.read_csv(ids_file)["document_id"]
                .astype(str)
                .tolist()
            )

            current_ids = (
                data[self.embedding_id_column]
                .astype(str)
                .tolist()
            )

            if (
                    len(cached_embeddings) == len(data)
                    and cached_ids == current_ids
            ):
                self.logger.info(
                    "Embedding cache validated successfully."
                )

                return cached_embeddings

            self.logger.warning(
                "Embedding cache does not match "
                "the current dataframe. "
                "Regenerating embeddings."
            )

        # --------------------------------------------------
        # Generate embeddings
        # --------------------------------------------------

        self.logger.info(
            f"Generating embeddings from column "
            f"'{self.text_field}'..."
        )

        embedding_input = data.copy()

        embedding_input[
            self.embedding_model_wrapper.column
        ] = (
            data[self.text_field]
            .fillna("")
        )

        # BERTEmbeddingModel.transform() expects
        # the complete dataframe and reads self.column.
        embeddings = (
            self.embedding_model_wrapper
            .transform(embedding_input)
        )

        self.logger.info(
            f"Generated embeddings with shape "
            f"{embeddings.shape}"
        )

        # --------------------------------------------------
        # Save cache
        # --------------------------------------------------

        if self.cache_enabled:
            cache_base.parent.mkdir(
                parents=True,
                exist_ok=True
            )

            np.save(
                embedding_file,
                embeddings
            )

            document_ids = pd.DataFrame({
                "row_index": range(len(data)),
                "document_id": (
                    data[
                        self.embedding_id_column
                    ]
                    .astype(str)
                )
            })

            document_ids.to_csv(
                ids_file,
                index=False
            )

            cfg = self.model_params[
                "embedding_model"
            ]

            metadata = {
                "model_name": cfg.get(
                    "model_name"
                ),
                "documents": len(data),
                "embedding_dimensions": int(
                    embeddings.shape[1]
                ),
                "id_column": (
                    self.embedding_id_column
                ),
                "text_field": self.text_field,
                "chunking": cfg.get(
                    "chunking",
                    {}
                ),

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
                f"Saved embedding cache: "
                f"{embedding_file}"
            )

        return embeddings

    # --------------------------------------------------
    # Execute
    # --------------------------------------------------

    def execute(
        self,
        data=None
    ):

        if data is None:
            raise ValueError(
                "Data must be provided to "
                "EmbeddingPipeline."
            )

        if not isinstance(
            data,
            pd.DataFrame
        ):

            raise TypeError(
                "EmbeddingPipeline expects "
                "a pandas DataFrame."
            )

        self.logger.info(
            f"Input dataframe shape: "
            f"{data.shape}"
        )


        # Do not modify original dataframe
        working_data = data.copy()

        # --------------------------------------------------
        # Validate fields
        # --------------------------------------------------

        if self.text_field not in working_data.columns:

            raise ValueError(
                f"Text field '{self.text_field}' "
                f"not found in dataframe."
            )

        if self.sort_columns:

            missing_columns = [
                column
                for column in self.sort_columns
                if column not in working_data.columns
            ]

            if missing_columns:

                raise ValueError(
                    f"Sort columns not found: "
                    f"{missing_columns}"
                )

            working_data = (
                working_data
                .sort_values(
                    self.sort_columns
                )
                .reset_index(drop=True)
            )

            # --------------------------------------------------
            # Generate hash to validate if deterministic
            # --------------------------------------------------
            input_hash = self._hash_inputs(
                working_data
            )

            self.logger.info(
                f"Embedding input hash: {input_hash}"
            )
        # --------------------------------------------------
        # Generate embeddings
        # --------------------------------------------------

        embeddings = self._get_embeddings(
            working_data
        )

        # --------------------------------------------------
        # Generate hash to validate if deterministic
        # --------------------------------------------------

        embedding_hash = self._hash_embeddings(
            embeddings
        )

        self.logger.info(
            f"Embedding output hash: {embedding_hash}"
        )

        # --------------------------------------------------
        # Add embeddings to dataframe
        # --------------------------------------------------


        embedding_column = (
            self.model_params[
                "embedding_model"
            ].get(
                "column",
                "embeddings"
            )
        )

        working_data[
            embedding_column
        ] = list(embeddings)

        self.logger.info(
            f"Added embedding column "
            f"'{embedding_column}'"
        )

        self.logger.info(
            f"Output dataframe shape: "
            f"{working_data.shape}"
        )

        return working_data

    def _hash_inputs(self, data):
        """
        Generate a deterministic hash of the exact text
        supplied to the embedding model.
        """

        texts = (
            data[self.text_field]
            .fillna("")
            .astype(str)
            .tolist()
        )

        hasher = hashlib.sha256()

        for text in texts:
            encoded = text.encode("utf-8")

            # Include length so boundaries between strings
            # cannot become ambiguous.
            hasher.update(
                len(encoded).to_bytes(
                    8,
                    byteorder="big"
                )
            )

            hasher.update(encoded)

        return hasher.hexdigest()

    def _hash_embeddings(self, embeddings):
        """
        Generate a deterministic hash of the embedding matrix.
        """

        contiguous_embeddings = np.ascontiguousarray(
            embeddings
        )

        return hashlib.sha256(
            contiguous_embeddings.tobytes()
        ).hexdigest()