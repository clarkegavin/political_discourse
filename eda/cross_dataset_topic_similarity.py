# eda/cross_dataset_topic_similarity.py

import os
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from sklearn.metrics.pairwise import cosine_similarity

from .base import EDAComponent
from logs.logger import get_logger
from visualisations.factory import VisualisationFactory


class CrossDatasetTopicSimilarityEDA(EDAComponent):
    """
    Compare topic embeddings across two datasets using cosine similarity.

    For every source topic, cosine similarity is calculated against every
    target topic. The top-N target matches are retained.

    The component also produces:
        - best-match diagnostics
        - similarity distribution statistics
        - similarity bands
        - optional histogram visualisation
        - optional CSV output
        - optional database persistence

    Expected input:
        One dataframe containing both datasets, with one embedding vector
        per row.
    """

    def __init__(
        self,
        dataset_field="Dataset",
        embedding_field="embeddings",

        source_dataset=None,
        target_dataset=None,

        source_unique_id="Identifier",
        target_unique_id="Identifier",

        source_topic_id="TopicId",
        target_topic_id="TopicId",
        source_topic_id_filter = None,

        source_topic_theme="TopicTheme",
        target_topic_theme="TopicTheme",

        source_description="TopicDescription",
        target_description="TopicDescription",

        top_n_matches=1000,
        similarity_threshold=0.0,

        similarity_bands=None,

        save_results=True,
        output_filename="cross_dataset_topic_similarity.csv",

        save_diagnostics=True,
        diagnostic_filename="cross_dataset_topic_similarity_diagnostics.csv",

        save_distribution=True,
        distribution_filename="cross_dataset_similarity_distribution.csv",

        save_bands=True,
        bands_filename="cross_dataset_similarity_bands.csv",

        saver_name=None,
        table_name=None,
        if_exists="replace",
        chunk_size=1000,
        schema=None,
        connector_params=None,

        **kwargs
    ):

        self.logger = get_logger(
            self.__class__.__name__
        )

        self.dataset_field = dataset_field
        self.embedding_field = embedding_field

        self.source_dataset = source_dataset
        self.target_dataset = target_dataset

        self.source_unique_id = source_unique_id
        self.target_unique_id = target_unique_id

        self.source_topic_id = source_topic_id
        self.target_topic_id = target_topic_id

        # Optional filter
        self.source_topic_id_filter = source_topic_id_filter
        self.logger.info(f"Source topic id filters: {self.source_topic_id_filter}")

        self.source_topic_theme = source_topic_theme
        self.target_topic_theme = target_topic_theme

        self.source_description = source_description
        self.target_description = target_description

        self.top_n_matches = top_n_matches
        self.similarity_threshold = similarity_threshold

        # Default similarity bands
        self.similarity_bands = (
            similarity_bands
            if similarity_bands is not None
            else [
                {
                    "label": "< 0.30",
                    "min": -1.0,
                    "max": 0.30
                },
                {
                    "label": "0.30–0.39",
                    "min": 0.30,
                    "max": 0.40
                },
                {
                    "label": "0.40–0.49",
                    "min": 0.40,
                    "max": 0.50
                },
                {
                    "label": "0.50–0.59",
                    "min": 0.50,
                    "max": 0.60
                },
                {
                    "label": "≥ 0.60",
                    "min": 0.60,
                    "max": 1.01
                }
            ]
        )

        self.save_results = save_results
        self.output_filename = output_filename

        self.save_diagnostics = save_diagnostics
        self.diagnostic_filename = diagnostic_filename

        self.save_distribution = save_distribution
        self.distribution_filename = distribution_filename

        self.save_bands = save_bands
        self.bands_filename = bands_filename

        # Database configuration
        self.saver_name = saver_name
        self.table_name = table_name
        self.if_exists = if_exists
        self.chunk_size = chunk_size
        self.schema = schema
        self.connector_params = connector_params or {}

        self.visualisation_factory = VisualisationFactory()

        self.logger.info(
            "Initialized CrossDatasetTopicSimilarityEDA "
            f"with source={source_dataset}, "
            f"target={target_dataset}, "
            f"top_n_matches={top_n_matches}, "
            f"threshold={similarity_threshold}"
        )

        self.logger.info(
            "Database parameters: "
            f"saver_name={self.saver_name}, "
            f"table_name={self.table_name}, "
            f"if_exists={self.if_exists}, "
            f"chunk_size={self.chunk_size}, "
            f"schema={self.schema}, "
            f"connector_params={self.connector_params}"
        )

    # ------------------------------------------------------------------
    # Main execution
    # ------------------------------------------------------------------

    def run(
        self,
        data,
        target=None,
        text_field=None,
        save_path=None,
        **kwargs
    ):

        if data is None:
            raise ValueError(
                "Data must be provided to CrossDatasetTopicSimilarityEDA."
            )

        if save_path is None:
            save_path = os.getcwd()

        os.makedirs(
            save_path,
            exist_ok=True
        )

        # --------------------------------------------------------------
        # Resolve configuration
        # --------------------------------------------------------------

        dataset_field = kwargs.get(
            "dataset_field",
            self.dataset_field
        )

        embedding_field = kwargs.get(
            "embedding_field",
            self.embedding_field
        )

        source_dataset = kwargs.get(
            "source_dataset",
            self.source_dataset
        )

        target_dataset = kwargs.get(
            "target_dataset",
            self.target_dataset
        )

        top_n_matches = kwargs.get(
            "top_n_matches",
            self.top_n_matches
        )

        similarity_threshold = kwargs.get(
            "similarity_threshold",
            self.similarity_threshold
        )

        similarity_bands = kwargs.get(
            "similarity_bands",
            self.similarity_bands
        )

        self.source_topic_id_filter = kwargs.get('source_topic_id_filter')
        self.logger.info(f"Source topic filter id: {self.source_topic_id_filter}")

        self.saver_name = kwargs.get('saver_name')
        self.table_name = kwargs.get('table_name')
        self.if_exists = kwargs.get('if_exists', 'replace')
        self.chunk_size = kwargs.get('chunk_size', 1000)
        self.schema = kwargs.get('schema')
        self.connector_params = kwargs.get('connector_params', {}) or {}

        self.diagnostic_filename = kwargs.get('diagnostic_filename', self.diagnostic_filename)
        self.output_filename = kwargs.get('output_filename', self.output_filename)
        self.distribution_filename = kwargs.get('distribution_filename', self.distribution_filename)
        self.bands_filename = kwargs.get('bands_filename', self.bands_filename)

        # --------------------------------------------------------------
        # Validate configuration
        # --------------------------------------------------------------

        required_columns = [
            dataset_field,
            embedding_field,
            self.source_unique_id,
            self.target_unique_id,
            self.source_topic_id,
            self.target_topic_id,
            self.source_topic_theme,
            self.target_topic_theme,
            self.source_description,
            self.target_description,
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in data.columns
        ]

        if missing_columns:
            raise ValueError(
                "CrossDatasetTopicSimilarityEDA is missing required "
                f"columns: {missing_columns}"
            )

        if not source_dataset:
            raise ValueError(
                "source_dataset must be configured."
            )

        if not target_dataset:
            raise ValueError(
                "target_dataset must be configured."
            )

        # --------------------------------------------------------------
        # Split datasets
        # --------------------------------------------------------------

        working_data = data.copy()

        source_data = (
            working_data[
                working_data[dataset_field]
                == source_dataset
            ]
            .copy()
        )

        # Optional topic ID filter
        self.logger.info(f"Source topic filter: {self.source_topic_id_filter}")
        if self.source_topic_id_filter is not None:
            source_data = source_data[
                source_data["TopicId"].isin(self.source_topic_id_filter)
            ].copy()

        target_data = (
            working_data[
                working_data[dataset_field]
                == target_dataset
            ]
            .copy()
        )

        self.logger.info(
            f"Source dataset '{source_dataset}': "
            f"{len(source_data)} rows"
        )

        self.logger.info(
            f"Target dataset '{target_dataset}': "
            f"{len(target_data)} rows"
        )

        if source_data.empty:
            raise ValueError(
                f"No rows found for source dataset '{source_dataset}'."
            )

        if target_data.empty:
            raise ValueError(
                f"No rows found for target dataset '{target_dataset}'."
            )

        # --------------------------------------------------------------
        # Prepare embeddings
        # --------------------------------------------------------------

        source_embeddings = self._prepare_embeddings(
            source_data,
            embedding_field,
            "source"
        )

        target_embeddings = self._prepare_embeddings(
            target_data,
            embedding_field,
            "target"
        )

        self.logger.info(
            f"Source embedding matrix shape: "
            f"{source_embeddings.shape}"
        )

        self.logger.info(
            f"Target embedding matrix shape: "
            f"{target_embeddings.shape}"
        )

        # --------------------------------------------------------------
        # Calculate cross-dataset cosine similarity
        # --------------------------------------------------------------

        self.logger.info(
            "Calculating cross-dataset cosine similarity..."
        )

        similarity_matrix = cosine_similarity(
            source_embeddings,
            target_embeddings
        )

        self.logger.info(
            f"Similarity matrix shape: "
            f"{similarity_matrix.shape}"
        )

        # --------------------------------------------------------------
        # Build ranked matches
        # --------------------------------------------------------------

        results = self._build_match_results(
            source_data=source_data,
            target_data=target_data,
            similarity_matrix=similarity_matrix,
            top_n_matches=top_n_matches,
            similarity_threshold=similarity_threshold
        )

        self.logger.info(f"Raw results length: {len(results)}")

        self.logger.info(
            f"Results per source topic:\n"
            f"{results.groupby('SourceTopicId').size()}"
        )

        # --------------------------------------------------------------
        # Diagnostics
        # --------------------------------------------------------------

        diagnostics = self._build_diagnostics(
            source_data=source_data,
            target_data=target_data,
            similarity_matrix=similarity_matrix
        )

        # --------------------------------------------------------------
        # Similarity distribution
        # --------------------------------------------------------------

        distribution = self._build_distribution(
            similarity_matrix=similarity_matrix
        )

        # --------------------------------------------------------------
        # Similarity bands
        # --------------------------------------------------------------

        bands = self._build_similarity_bands(
            diagnostics=diagnostics,
            similarity_bands=similarity_bands
        )

        # --------------------------------------------------------------
        # Save outputs
        # --------------------------------------------------------------

        if self.save_results:
            results_path = os.path.join(
                save_path,
                self.output_filename
            )

            results.to_csv(
                results_path,
                index=False,
                encoding='utf-8-sig'
            )

            self.logger.info(
                f"Saved similarity results to {results_path}"
            )

        if self.save_diagnostics:
            diagnostics_path = os.path.join(
                save_path,
                self.diagnostic_filename
            )

            diagnostics.to_csv(
                diagnostics_path,
                index=False,
                encoding='utf-8-sig'
            )

            self.logger.info(
                f"Saved similarity diagnostics to "
                f"{diagnostics_path}"
            )

        if self.save_distribution:
            distribution_path = os.path.join(
                save_path,
                self.distribution_filename
            )

            distribution.to_csv(
                distribution_path,
                index=False,
                encoding='utf-8-sig'
            )

            self.logger.info(
                f"Saved similarity distribution to "
                f"{distribution_path}"
            )

        if self.save_bands:
            bands_path = os.path.join(
                save_path,
                self.bands_filename
            )

            bands.to_csv(
                bands_path,
                index=False,
                encoding='utf-8-sig'
            )

            self.logger.info(
                f"Saved similarity bands to {bands_path}"
            )

        # --------------------------------------------------------------
        # Database output
        # --------------------------------------------------------------

        if self.saver_name and self.table_name:
            self._save_to_database(
                results
            )

        # --------------------------------------------------------------
        # Visualisations
        # --------------------------------------------------------------

        viz_params = kwargs.get(
            "viz_params",
            []
        )

        for viz in viz_params:

            viz_name = viz["name"]

            filename = viz.get(
                "filename",
                f"{viz_name}.png"
            )

            viz_config = {
                key: value
                for key, value in viz.items()
                if key not in [
                    "name",
                    "filename"
                ]
            }

            visualisation = (
                self.visualisation_factory
                .get_visualisation(
                    viz_name,
                    **viz_config
                )
            )

            if visualisation is None:
                raise ValueError(
                    f"Visualisation '{viz_name}' "
                    f"not found in VisualisationFactory"
                )

            # Histogram uses the best similarity distribution
            if viz_name == "similarity_histogram":

                fig, ax = visualisation.plot(
                    diagnostics,
                    **viz_config
                )
            elif viz_name == "similarity_line_plot":

                fig, ax = visualisation.plot(
                    data=results,
                    **viz_config
                )
            else:

                fig, ax = visualisation.plot(
                    data=diagnostics,
                    **viz_config
                )

            output_path = os.path.join(
                save_path,
                filename
            )

            fig.savefig(
                output_path,
                dpi=300,
                bbox_inches="tight"
            )

            plt.close(fig)

            self.logger.info(
                f"Saved visualisation to {output_path}"
            )

        # --------------------------------------------------------------
        # Log diagnostic summary
        # --------------------------------------------------------------

        self._log_summary(
            diagnostics,
            bands
        )

        # --------------------------------------------------------------
        # Return everything useful to downstream stages
        # --------------------------------------------------------------

        return {
            "matches": results,
            "diagnostics": diagnostics,
            "distribution": distribution,
            "bands": bands
        }

    # ------------------------------------------------------------------
    # Embedding preparation
    # ------------------------------------------------------------------

    def _prepare_embeddings(
        self,
        dataframe,
        embedding_field,
        dataset_label
    ):

        embeddings = dataframe[
            embedding_field
        ].tolist()

        if not embeddings:
            raise ValueError(
                f"No embeddings found for {dataset_label} dataset."
            )

        try:
            matrix = np.vstack(
                embeddings
            ).astype(
                np.float32
            )

        except Exception as e:

            raise ValueError(
                f"Could not convert {dataset_label} embeddings "
                f"into a matrix: {e}"
            )

        if matrix.ndim != 2:
            raise ValueError(
                f"{dataset_label} embeddings must form a "
                f"2-dimensional matrix. Got shape "
                f"{matrix.shape}."
            )

        if not np.isfinite(matrix).all():
            raise ValueError(
                f"{dataset_label} embeddings contain NaN "
                f"or infinite values."
            )

        return matrix

    # ------------------------------------------------------------------
    # Match results
    # ------------------------------------------------------------------

    def _build_match_results(
        self,
        source_data,
        target_data,
        similarity_matrix,
        top_n_matches,
        similarity_threshold
    ):
        self.logger.info(f"Similarity matrix shape in _build_match_results: {similarity_matrix.shape}")

        rows = []

        top_n_matches = min(
            top_n_matches,
            similarity_matrix.shape[1]
        )

        for source_index in range(
            similarity_matrix.shape[0]
        ):

            similarities = similarity_matrix[
                source_index
            ]

            self.logger.info(
                f"Source index {source_index}: "
                f"min={similarities.min():.6f}, "
                f"max={similarities.max():.6f}, "
                f"negative={(similarities < 0).sum()}, "
                f"zero={(similarities == 0).sum()}"
            )

            # Sort descending
            ranked_indices = np.argsort(
                similarities
            )[::-1]

            rank = 0

            for target_index in ranked_indices:

                similarity = float(
                    similarities[target_index]
                )

                if (
                        similarity_threshold is not None
                        and similarity < similarity_threshold
                ):
                    continue

                rank += 1

                source_row = (
                    source_data.iloc[source_index]
                )

                target_row = (
                    target_data.iloc[target_index]
                )

                rows.append(
                    {
                        "SourceDataset":
                            source_row[self.dataset_field],

                        "SourceUniqueId":
                            source_row[self.source_unique_id],

                        "SourceTopicId":
                            source_row[self.source_topic_id],

                        "SourceTopicTheme":
                            source_row[self.source_topic_theme],

                        "SourceTopicDescription":
                            source_row[self.source_description],

                        "TargetDataset":
                            target_row[self.dataset_field],

                        "TargetUniqueId":
                            target_row[self.target_unique_id],

                        "TargetTopicId":
                            target_row[self.target_topic_id],

                        "TargetTopicTheme":
                            target_row[self.target_topic_theme],

                        "TargetTopicDescription":
                            target_row[self.target_description],

                        "Similarity":
                            similarity,

                        "Rank":
                            rank
                    }
                )

                if rank >= top_n_matches:
                    break

        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def _build_diagnostics(
        self,
        source_data,
        target_data,
        similarity_matrix
    ):

        rows = []

        for source_index in range(
            similarity_matrix.shape[0]
        ):

            similarities = (
                similarity_matrix[source_index]
            )

            source_row = (
                source_data.iloc[source_index]
            )

            best_index = int(
                np.argmax(similarities)
            )

            best_similarity = float(
                similarities[best_index]
            )

            target_row = (
                target_data.iloc[best_index]
            )

            rows.append(
                {
                    "SourceDataset":
                        source_row[self.dataset_field],

                    "SourceUniqueId":
                        source_row[self.source_unique_id],

                    "SourceTopicId":
                        source_row[self.source_topic_id],

                    "SourceTopicTheme":
                        source_row[self.source_topic_theme],

                    "SourceTopicDescription":
                        source_row[self.source_description],

                    "BestTargetUniqueId":
                        target_row[self.target_unique_id],

                    "BestTargetTopicId":
                        target_row[self.target_topic_id],

                    "BestTargetTopicTheme":
                        target_row[self.target_topic_theme],

                    "BestTargetTopicDescription":
                        target_row[self.target_description],

                    "BestSimilarity":
                        best_similarity,

                    "MeanSimilarity":
                        float(np.mean(similarities)),

                    "MedianSimilarity":
                        float(np.median(similarities)),

                    "StdSimilarity":
                        float(np.std(similarities)),

                    "MinSimilarity":
                        float(np.min(similarities)),

                    "MaxSimilarity":
                        float(np.max(similarities))
                }
            )

        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Distribution
    # ------------------------------------------------------------------

    def _build_distribution(
        self,
        similarity_matrix
    ):

        similarities = (
            similarity_matrix.flatten()
        )

        return pd.DataFrame(
            [
                {
                    "Statistic": "count",
                    "Value": len(similarities)
                },
                {
                    "Statistic": "mean",
                    "Value": float(
                        np.mean(similarities)
                    )
                },
                {
                    "Statistic": "median",
                    "Value": float(
                        np.median(similarities)
                    )
                },
                {
                    "Statistic": "std",
                    "Value": float(
                        np.std(similarities)
                    )
                },
                {
                    "Statistic": "min",
                    "Value": float(
                        np.min(similarities)
                    )
                },
                {
                    "Statistic": "max",
                    "Value": float(
                        np.max(similarities)
                    )
                },
                {
                    "Statistic": "q25",
                    "Value": float(
                        np.percentile(
                            similarities,
                            25
                        )
                    )
                },
                {
                    "Statistic": "q75",
                    "Value": float(
                        np.percentile(
                            similarities,
                            75
                        )
                    )
                },
                {
                    "Statistic": "q90",
                    "Value": float(
                        np.percentile(
                            similarities,
                            90
                        )
                    )
                },
                {
                    "Statistic": "q95",
                    "Value": float(
                        np.percentile(
                            similarities,
                            95
                        )
                    )
                },
                {
                    "Statistic": "q99",
                    "Value": float(
                        np.percentile(
                            similarities,
                            99
                        )
                    )
                }
            ]
        )

    # ------------------------------------------------------------------
    # Similarity bands
    # ------------------------------------------------------------------

    def _build_similarity_bands(
        self,
        diagnostics,
        similarity_bands
    ):

        similarities = (
            diagnostics["BestSimilarity"]
        )

        rows = []

        total = len(similarities)

        for band in similarity_bands:

            minimum = band["min"]
            maximum = band["max"]

            if minimum < maximum:

                mask = (
                    (similarities >= minimum)
                    &
                    (similarities < maximum)
                )

            else:

                mask = (
                    similarities >= minimum
                )

            count = int(
                mask.sum()
            )

            percentage = (
                (count / total) * 100
                if total
                else 0
            )

            rows.append(
                {
                    "Band": band["label"],
                    "Minimum": minimum,
                    "Maximum": maximum,
                    "Count": count,
                    "Percentage": percentage
                }
            )

        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    def _log_summary(
        self,
        diagnostics,
        bands
    ):

        if diagnostics.empty:
            return

        self.logger.info(
            "Cross-dataset similarity summary:"
        )

        self.logger.info(
            f"Source topics: {len(diagnostics)}"
        )

        self.logger.info(
            "Best similarity mean: "
            f"{diagnostics['BestSimilarity'].mean():.4f}"
        )

        self.logger.info(
            "Best similarity median: "
            f"{diagnostics['BestSimilarity'].median():.4f}"
        )

        self.logger.info(
            "Best similarity minimum: "
            f"{diagnostics['BestSimilarity'].min():.4f}"
        )

        self.logger.info(
            "Best similarity maximum: "
            f"{diagnostics['BestSimilarity'].max():.4f}"
        )

        self.logger.info(
            "Similarity bands:\n"
            + bands.to_string(index=False)
        )

    # ------------------------------------------------------------------
    # Database persistence
    # ------------------------------------------------------------------

    def _save_to_database(
        self,
        dataframe
    ):

        self.logger.info(
            f"Saving similarity results using saver "
            f"'{self.saver_name}' to "
            f"{self.schema or 'dbo'}.{self.table_name}"
        )

        # Import here so that the EDA component does not require
        # database infrastructure unless database saving is requested.
        from data.savers.factory import DataSaverFactory
        from data.sqlalchemy_connector import SQLAlchemyConnector

        # Optionally save the term-frequency DataFrame using the DataSaverFactory (reuse DataSaverPipeline behaviour)
        self.logger.info(f"Saving results to database table '{self.table_name}' using saver '{self.saver_name}'")
        if self.table_name:
            # Use provided saver_name or default to 'sql_server'
            saver_name = self.saver_name or 'sql_server'
            self.logger.info(f"Saving DataFrame to table '{self.table_name}' using saver '{saver_name}'")
            saver = DataSaverFactory.get_saver(saver_name)
            if saver is None:
                self.logger.error(f"Data saver '{saver_name}' not found in DataSaverFactory")
                raise ValueError(f"Data saver '{saver_name}' not found in DataSaverFactory")

            # Instantiate connector
            try:
                connector = SQLAlchemyConnector(
                    **self.connector_params) if self.connector_params is not None else SQLAlchemyConnector()
            except Exception as e:
                self.logger.error(f"Failed to initialize SQLAlchemyConnector: {e}")
                raise

            try:
                saver.save(
                    df=dataframe,
                    table_name=self.table_name,
                    connector=connector,
                    if_exists=self.if_exists,
                    chunk_size=self.chunk_size,
                    schema=self.schema,
                )
            except Exception as e:
                self.logger.error(f"Failed to save DataFrame: {e}")
                raise