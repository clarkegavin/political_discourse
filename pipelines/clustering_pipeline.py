# pipelines/clustering_pipeline.py

import numpy as np
import os
import pandas as pd

from collections import Counter

from visualisations import VisualisationFactory
from .base import Pipeline
from logs.logger import get_logger

from vectorizers import VectorizerFactory
from embedding_models import EmbeddingModelFactory
from models import ModelFactory
from reducers import ReducerFactory
from evaluators import EvaluatorFactory
import hashlib


class ClusteringPipeline(Pipeline):

    def __init__(self, **params):

        super().__init__(
            name=params.get("name", "clustering_pipeline")
        )

        self.logger = get_logger("ClusteringPipeline")

        self.name = params.get(
            "name",
            "clustering_pipeline"
        )

        self.logger.info(
            f"Initializing ClusteringPipeline with name: {self.name}"
        )

        # ---------------------------------------------------------
        # Text configuration
        # ---------------------------------------------------------

        self.text_fields = params.get(
            "text_fields",
            []
        )

        self.combined_text_field = params.get(
            "combined_text_field",
            "clustering_text"
        )


        self.genre_field = params.get(
            "genre_field"
        )

        self.filter_genre = params.get(
            "filter_genre"
        )

        self.columns_to_drop = params.get(
            "columns_to_drop",
            []
        )

        self.sort_columns = params.get("sort_columns", [])

        # ---------------------------------------------------------
        # Representation model
        #
        # Either:
        #   - vectorizer
        # or
        #   - embedding_model
        #
        # but not both.
        # ---------------------------------------------------------

        self.vectorizer = None
        self.embedding_model = None

        vectorizer_cfg = params.get(
            "vectorizer"
        )

        embedding_cfg = params.get(
            "embedding_model"
        )

        if vectorizer_cfg and embedding_cfg:
            raise ValueError(
                "ClusteringPipeline cannot configure both "
                "'vectorizer' and 'embedding_model'. "
                "Specify one representation method."
            )

        if not vectorizer_cfg and not embedding_cfg:
            raise ValueError(
                "ClusteringPipeline requires either "
                "'vectorizer' or 'embedding_model'."
            )

        # ---------------------------------------------------------
        # TF-IDF vectorizer
        # ---------------------------------------------------------

        if vectorizer_cfg:

            vectorizer_name = vectorizer_cfg.get(
                "vectorizer_name"
            )

            vectorizer_field = vectorizer_cfg.get(
                "vectorizer_field"
            )

            vectorizer_params = dict(
                vectorizer_cfg.get(
                    "vectorizer_params",
                    {}
                )
            )

            self.logger.info(
                f"Setting up vectorizer "
                f"'{vectorizer_name}' "
                f"for field '{vectorizer_field}' "
                f"with params {vectorizer_params}"
            )

            vectorizer_params["column"] = vectorizer_field

            self.vectorizer = (
                VectorizerFactory.get_vectorizer(
                    vectorizer_name,
                    **vectorizer_params
                )
            )

        # ---------------------------------------------------------
        # Embedding model
        # ---------------------------------------------------------

        elif embedding_cfg:

            embedding_name = embedding_cfg.get(
                "name"
            )

            embedding_params = dict(
                embedding_cfg
                )

            # 'name' is used by the factory, so remove it
            # from the kwargs passed to the model.
            embedding_params.pop(
                "name",
                None
            )

            self.logger.info(
                f"Setting up embedding model "
                f"'{embedding_name}' "
                f"with params {embedding_params}"
            )

            self.embedding_model = (
                EmbeddingModelFactory.get_embedding_model(
                    embedding_name,
                    **embedding_params
                )
            )

            if self.embedding_model is None:
                raise ValueError(
                    f"Embedding model '{embedding_name}' "
                    f"could not be created."
                )

        # ---------------------------------------------------------
        # Clusterer
        # ---------------------------------------------------------

        clusterer_cfg = params.get(
            "clusterer",
            {}
        )

        clusterer_name = clusterer_cfg.get(
            "name"
        )

        clusterer_params = clusterer_cfg.get(
            "params",
            {}
        )

        self.logger.info(
            f"Setting up clusterer "
            f"'{clusterer_name}' "
            f"with params {clusterer_params}"
        )

        self.clusterer = ModelFactory.get_model(
            clusterer_name,
            **clusterer_params
        )

        # ---------------------------------------------------------
        # Reducer
        # ---------------------------------------------------------

        reducer_cfg = params.get(
            "reducer",
            []
        )

        self.reducers = ReducerFactory.get_reducers(
            reducer_cfg
        )

        # ---------------------------------------------------------
        # Visualisation
        # ---------------------------------------------------------

        visualisations_cfg = params.get(
            "visualisations",
            {}
        )

        visualisations_name = visualisations_cfg.get(
            "name"
        )

        visualisations_params = visualisations_cfg.get(
            "params",
            {}
        )

        self.dimensions = visualisations_params.get(
            "dimensions",
            2
        )

        self.plotter = (
            VisualisationFactory.get_visualisation(
                visualisations_name,
                **visualisations_params
            )
        )

        # ---------------------------------------------------------
        # Evaluators
        # ---------------------------------------------------------

        self.evaluators = []

        for cfg in params.get(
            "evaluators",
            []
        ):

            self.logger.info(
                f"Setting up evaluator '{cfg}'"
            )

            evaluator = (
                EvaluatorFactory.get_evaluator(
                    cfg["name"],
                    params=cfg.get(
                        "params",
                        {}
                    ),
                    plotter_name=visualisations_name,
                    plotter_params=visualisations_params
                )
            )

            if evaluator:
                self.evaluators.append(
                    (evaluator, cfg)
                )

    # =============================================================
    # Execute
    # =============================================================

    def execute(self, df=None):

        self.logger.info(
            "Starting clustering pipeline"
        )

        if df is None:
            raise ValueError(
                "ClusteringPipeline requires a dataframe."
            )


        # ---------------------------------------------------------
        # Preserve original dataframe
        # ---------------------------------------------------------

        df_original = df.copy()

        # ---------------------------------------------------------
        # Sort columns for deterministic behaviour
        # ---------------------------------------------------------
        self.logger.info(f"Preparing data for sorting")
        if self.sort_columns:
            df_original = self._sort_dataframe(
                df_original,
                self.sort_columns
            )

        self.logger.info(
            f"First 20 TopicIds: {df_original['TopicId'].head(20).tolist()}"
        )
        # ---------------------------------------------------------
        # Drop configured columns
        # ---------------------------------------------------------

        cols_to_drop = [
            col
            for col in self.columns_to_drop
            if col in df.columns
        ]

        df_for_clustering = (
            df_original.drop(
                columns=cols_to_drop,
                errors="ignore"
            )
        )

        # ---------------------------------------------------------
        # Prepare combined text field
        # ---------------------------------------------------------

        df_for_clustering = self._prepare_text(
            df_for_clustering
        )

        self.logger.info(
            f"Dropped columns {cols_to_drop}, "
            f"shape for clustering: "
            f"{df_for_clustering.shape}"
        )

        # ---------------------------------------------------------
        # Create representation
        #
        # Keep the original representation separate because:
        #
        #   X_vectorized = TF-IDF representation
        #   X_embedded   = embedding representation
        #
        # X_cluster_values is the representation currently
        # being passed through dimensionality reduction.
        # ---------------------------------------------------------

        X_vectorized = None
        X_embedded = None

        if self.embedding_model is not None:

            self.logger.info(
                f"Embedding texts using "
                f"{self.embedding_model.name}"
            )
            # TEMPORARY DIAGNOSTIC
            text_hash = hashlib.sha256(
                "\n".join(
                    df_for_clustering["clustering_text"]
                    .fillna("")
                    .astype(str)
                    .tolist()
                ).encode("utf-8")
            ).hexdigest()

            self.logger.info(
                f"Clustering text hash: {text_hash}"
            )

            # END TEMPORARY DIAGNOSTIC
            X_embedded = (
                self.embedding_model.fit_transform(
                    df_for_clustering
                )
            )
            # TEMPORARY DIAGNOSTIC
            np.save(
                os.path.join(
                    self.plotter.output_dir,
                    "embeddings_current.npy"
                ),
                X_embedded
            )
            # END TEMPORARY DIAGNOSTIC
            self.logger.info(
                f"Embedded shape: "
                f"{X_embedded.shape}"
            )

            # TEMPORARY DIAGNOSTIC
            # Hash embeddings to check reproducibility

            embedding_hash = hashlib.sha256(
                np.ascontiguousarray(X_embedded).tobytes()
            ).hexdigest()

            self.logger.info(
                f"Embedding matrix hash: {embedding_hash}"
            )
            # END TEMPORARY DIAGNOSTIC
            X_cluster_values = self._to_numpy(
                X_embedded
            )

        elif self.vectorizer is not None:

            self.logger.info(
                f"Vectorizing texts using "
                f"{self.vectorizer.name}"
            )

            X_vectorized = (
                self.vectorizer.fit_transform(
                    df_for_clustering
                )
            )

            self.logger.info(
                f"Vectorized shape: "
                f"{X_vectorized.shape}"
            )

            X_cluster_values = self._to_numpy(
                X_vectorized
            )

        else:

            raise RuntimeError(
                "No vectorizer or embedding model "
                "configured."
            )

        # ---------------------------------------------------------
        # Dimensionality reduction
        # ---------------------------------------------------------

        for reducer in self.reducers:

            self.logger.info(
                f"Reducing dimensions using "
                f"{reducer.name}"
            )

            X_cluster_values = (
                reducer.fit_transform(
                    X_cluster_values
                )
            )

            X_cluster_values = self._to_numpy(
                X_cluster_values
            )

            self.logger.info(
                f"Shape after {reducer.name}: "
                f"{X_cluster_values.shape}"
            )

            umap_hash = hashlib.sha256(
                np.ascontiguousarray(X_cluster_values).tobytes()
            ).hexdigest()

            self.logger.info(
                f"UMAP matrix hash: {umap_hash}"
            )

        # ---------------------------------------------------------
        # Clustering
        # ---------------------------------------------------------

        self.logger.info(
            f"Clustering using "
            f"{self.clusterer.name}"
        )

        labels = self.clusterer.fit_predict(
            X_cluster_values
        )

        probabilities = (
            self.clusterer.probabilities_
            if hasattr(
                self.clusterer,
                "probabilities_"
            )
            else None
        )

        self.logger.info(
            f"Cluster labels assigned: "
            f"{set(labels)}"
        )

        if probabilities is not None:

            self.logger.info(
                f"Cluster probabilities shape: "
                f"{probabilities.shape}"
            )

        else:

            self.logger.info(
                "Cluster probabilities not available "
                "for this clusterer"
            )

        # ---------------------------------------------------------
        # Reduce for visualisation
        # ---------------------------------------------------------

        viz_reducer = (
            ReducerFactory.create_reducer(
                name="umap",
                n_components=self.dimensions
            )
        )

        self.logger.info(
            f"Reducing dimensions using "
            f"{viz_reducer}"
        )

        X_reduced = viz_reducer.fit_transform(
            X_cluster_values
        )

        X_reduced = self._to_numpy(
            X_reduced
        )

        self.logger.info(
            f"Reduced shape: "
            f"{X_reduced.shape}"
        )

        # ---------------------------------------------------------
        # Plot
        # ---------------------------------------------------------

        self.logger.info(
            f"Plotting clusters using "
            f"{self.plotter.name}"
        )

        fig, ax, scatter = self.plotter.plot(
            X_reduced,
            labels,
            metadata=df_original
        )

        self.logger.info(
            "Cluster plot generated"
        )

        plot_path = os.path.join(
            self.plotter.output_dir,
            f"{self.name}_cluster_plot.png"
        )

        self.plotter.save(
            fig,
            plot_path
        )

        self.plotter.save_embeddings(
            X_reduced,
            labels,
            df_original,
            prefix=f"{self.name}_clustering_pipeline"
        )

        self.logger.info(
            f"Cluster plot saved as "
            f"'{self.name}_cluster_plot.png'"
        )

        self.plotter.save_interactive_plot(
            X_reduced,
            labels,
            prefix=f"{self.name}_cluster_plot"
        )

        if probabilities is not None:

            self.plotter.save_interactive_plot_by_probability(
                X_reduced,
                labels,
                probabilities,
                prefix=f"{self.name}_cluster_plot_by_probability"
            )

        # ---------------------------------------------------------
        # Attach cluster labels
        # ---------------------------------------------------------

        df_original["cluster"] = labels

        for col in df_original.columns:

            if isinstance(
                df_original[col].dtype,
                pd.Int64Dtype
            ):

                df_original[col] = (
                    df_original[col].astype(float)
                )

        # ---------------------------------------------------------
        # Save clustered dataframe
        # ---------------------------------------------------------

        output_path = os.path.join(
            self.plotter.output_dir,
            f"{self.name}_clustered_data.csv"
        )

        try:

            df_original.to_csv(
                output_path,
                index=False,
                encoding="utf-8-sig",
                #errors="replace"
            )

            self.logger.info(
                f"Clustered data with labels saved "
                f"to {output_path}"
            )

        except Exception as e:

            self.logger.error(
                f"Error saving clustered data "
                f"to {output_path}: {e}"
            )

        # ---------------------------------------------------------
        # Evaluators
        # ---------------------------------------------------------

        for evaluator, cfg in self.evaluators:

            self.logger.info(
                f"Running evaluator "
                f"{evaluator.name} "
                f"with config {cfg}"
            )

            metrics = cfg.get(
                "metrics",
                []
            )

            params = cfg.get(
                "params",
                {}
            )

            if evaluator.name == "clustering_quality":

                self.logger.info(
                    "Evaluating clustering quality"
                )

                evaluator.evaluate(
                    X_cluster_values,
                    labels,
                    clusterer=self.clusterer,
                    metrics=metrics,
                    params=params
                )

            elif evaluator.name == "cluster_profile":

                self.logger.info(
                    "Evaluating cluster profile"
                )

                evaluator.evaluate(
                    df_original,
                    labels,
                    metrics=metrics,
                    params=params
                )

        # ---------------------------------------------------------
        # TF-IDF cluster keywords
        #
        # This is deliberately only performed for TF-IDF.
        # Embedding dimensions do not have meaningful keywords.
        # ---------------------------------------------------------

        if self.vectorizer is not None:

            cluster_keywords = (
                self._extract_cluster_keywords(
                    X_vectorized,
                    labels
                )
            )

            label_counts = Counter(
                labels
            )

            for cluster_id, keywords in (
                cluster_keywords.items()
            ):

                size = label_counts.get(
                    cluster_id,
                    0
                )

                self.logger.info(
                    f"Cluster {cluster_id} "
                    f"(size={size}) keywords: "
                    f"{keywords}"
                )

            keyword_path = os.path.join(
                self.plotter.output_dir,
                f"{self.name}_cluster_keywords.txt"
            )

            self._save_cluster_keywords(
                cluster_keywords,
                keyword_path
            )

        # ---------------------------------------------------------
        # Return
        # ---------------------------------------------------------

        return df_original

    # =============================================================
    # Helper methods
    # =============================================================

    def _save_cluster_keywords(
        self,
        cluster_keywords,
        filepath
    ):

        try:

            self.logger.info(
                f"Saving cluster keywords "
                f"to {filepath}"
            )

            with open(
                filepath,
                "w",
                encoding="utf-8",
                errors="replace"
            ) as f:

                for cluster_id, keywords in (
                    cluster_keywords.items()
                ):

                    f.write(
                        f"Cluster {cluster_id} "
                        f"keywords: "
                        f"{', '.join(keywords)}\n"
                    )

            self.logger.info(
                f"Cluster keywords saved "
                f"to {filepath}"
            )

        except Exception as e:

            self.logger.error(
                f"Error saving cluster keywords "
                f"to {filepath}: {e}"
            )

    def _extract_cluster_keywords(
        self,
        X,
        labels,
        top_n=10
    ):

        terms = (
            self.vectorizer.get_feature_names()
        )

        result = {}

        for cluster_id in set(labels):

            if cluster_id == -1:
                continue

            idx = np.where(
                labels == cluster_id
            )[0]

            cluster_matrix = X[idx]

            centroid = (
                cluster_matrix.mean(axis=0)
            )

            if hasattr(
                centroid,
                "A1"
            ):
                centroid = centroid.A1
            else:
                centroid = np.asarray(
                    centroid
                ).ravel()

            top_idx = centroid.argsort()[
                -top_n:
            ]

            result[cluster_id] = [
                terms[i]
                for i in top_idx
            ]

        return result

    def _to_numpy(self, X):

        if hasattr(
            X,
            "toarray"
        ):

            return X.toarray().astype(
                np.float32
            )

        if hasattr(
            X,
            "to_numpy"
        ):

            return X.to_numpy(
                dtype=np.float32,
                copy=False
            )

        return np.asarray(
            X,
            dtype=np.float32
        )

    def _prepare_text(self, df):

        text_fields = self.text_fields
        combined_field = self.combined_text_field

        missing = [
            c
            for c in text_fields
            if c not in df.columns
        ]

        if missing:

            raise ValueError(
                f"Configured text fields not found "
                f"in dataframe: {missing}"
            )

        df = df.copy()

        df[combined_field] = (
            df[text_fields]
            .fillna("")
            .astype(str)
            .agg(" ".join, axis=1)
            .str.replace(
                r"\s+",
                " ",
                regex=True
            )
            .str.strip()
        )

        return df

    def _sort_dataframe(self, df, columns):
        """
        Sort dataframe deterministically using configured columns.
        """
        self.logger.info(f"Sorting dataframe: {columns}")
        if not columns:
            return df

        missing = [
            column for column in columns
            if column not in df.columns
        ]

        if missing:
            raise ValueError(
                f"Sort columns not found in dataframe: {missing}"
            )

        return (
            df.sort_values(
                by=columns,
                kind="stable"
            )
            .reset_index(drop=True)
        )