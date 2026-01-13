#pipelines/clustering_pipeline.py
import numpy as np
import os
from visualisations import VisualisationFactory
from .base import Pipeline
from logs.logger import get_logger
from collections import Counter

#from vectorizers.tfidf_vectorizer import TfidfTextVectorizer
#from clusterers.hdbscan_clusterer import HDBSCANClusterer
#from reducers.umap_reducer import UMAPReducer
#from visualisations.cluster_plotter import ClusterPlotter

from vectorizers import VectorizerFactory
from models import ModelFactory
from reducers import ReducerFactory
from evaluators import EvaluatorFactory
import pandas as pd

class ClusteringPipeline(Pipeline):
    def __init__(self, **params):
        # Ensure parent initializer is called with a default name to satisfy base contract
        super().__init__(name=params.get("name", "clustering_pipeline"))
        self.logger = get_logger("ClusteringPipeline")

        self.name = params.get("name", "clustering_pipeline")
        self.logger.info(f"Initializing ClusteringPipeline with name: {self.name}")
        # params dict contains all YAML keys
        self.text_field = params.get("text_field")
        self.genre_field = params.get("genre_field")
        self.filter_genre = params.get("filter_genre")

        #Vectorizer
        vectorizer_cfg = params.get("vectorizer", {})
        vectorizer_name = vectorizer_cfg.get("vectorizer_name")
        vectorizer_field = vectorizer_cfg.get("vectorizer_field")
        vectorizer_params = vectorizer_cfg.get("vectorizer_params", {})
        self.logger.info(f"Setting up vectorizer '{vectorizer_name}' for field '{vectorizer_field}' with params {vectorizer_params}")
        vectorizer_params['column'] = vectorizer_field
        self.vectorizer = VectorizerFactory.get_vectorizer(vectorizer_name, **vectorizer_params)

        #Clusterer
        clusterer_cfg = params.get("clusterer", {})
        clusterer_name = clusterer_cfg.get("name")
        clusterer_params = clusterer_cfg.get("params", {})
        self.logger.info(f"Setting up clusterer '{clusterer_name}' with params {clusterer_params}")
        self.clusterer = ModelFactory.get_model(clusterer_name, **clusterer_params)

        #Reducer
        reducer_cfg = params.get("reducer", [])
        self.reducers = ReducerFactory.get_reducers(reducer_cfg)

        visualisations_cfg = params.get("visualisations", {})
        visualisations_name = visualisations_cfg.get("name")
        visualisations_params = visualisations_cfg.get("params", {})
        self.dimensions = visualisations_params.get("dimensions", 2)
        self.plotter = VisualisationFactory.get_visualisation(visualisations_name, **visualisations_params)

        # # Evaluator configuration
        # evaluator_cfg = params.get("evaluator", {}) or {}
        # # allow legacy keys at top-level
        # evaluator_name = params.get("evaluator_name") or evaluator_cfg.get("name") or params.get("evaluator_name")
        # self.evaluator_metrics = evaluator_cfg.get("metrics") or params.get("metrics") or []
        # self.evaluator_params = evaluator_cfg.get("params") or {}
        # self.evaluator = None
        # if evaluator_name:
        #     self.evaluator = EvaluatorFactory.get_evaluator(evaluator_name, plotter_name=visualisations_name, plotter_params=visualisations_params)

        self.evaluators = []
        for cfg in params.get("evaluators", []):
            self.logger.info(f"Setting up evaluator '{cfg}'")
            evaluator = EvaluatorFactory.get_evaluator(
                cfg["name"],
                params = cfg.get("params", {}),
                plotter_name=visualisations_name,
                plotter_params=visualisations_params
            )
            if evaluator:
                self.evaluators.append((evaluator, cfg))


    def execute(self, df=None):
        # match base signature (data=None) and accept dataframe
        self.logger.info("Starting clustering pipeline")

        # Filter genre
        # df_filtered = df[df[self.genre_field] == self.filter_genre].copy()
        # self.logger.info(f"Filtering records where {self.genre_field} == {self.filter_genre}")
        # self.logger.info(f"df_filtered type: {type(df_filtered)}")
        #
        # texts = df_filtered[self.text_field].fillna("").tolist()
        # self.logger.info(f"Filtered records: {len(texts)}")

        # Keep original dataframe with IDs for saving and descriptive stats
        df_original = df.copy()

        # drop id, appid and name columns if present for clustering.
        # Total Reviews/Total_Positive/Total_Negative are also dropped to avoid leakage as these form the success metrics
        cols_to_drop = [col for col in ['Id', 'AppId', 'Name', 'Total_Reviews', 'Total_Positive', 'Total_Negative'] if col in df.columns]
        df_for_clustering = df_original.drop(columns=cols_to_drop, errors='ignore')
        self.logger.info(f"Dropped columns {cols_to_drop}, shape for clustering: {df_for_clustering.shape}")

        # if cols_to_drop:
        #     self.logger.info(f"Dropping columns: {cols_to_drop}")
        #     df = df.drop(columns=cols_to_drop)
        # self.logger.info(f"DataFrame shape after dropping id/appid/name columns: {df.shape}")

        # Vectorize
        if self.vectorizer is not None:
            self.logger.info(f"Vectorizing texts using {self.vectorizer.name}")
            X_cluster = self.vectorizer.fit_transform(df_for_clustering)
            self.logger.info(f"Vectorized shape: {df.shape}")
        else:
            self.logger.info("No vectorizer configured, using original dataframe for clustering")
            X_cluster = df_for_clustering.copy()

        # Reduce
        X_cluster.columns = X_cluster.columns.astype(str) # ensure columns are str for reducers
        #if not already numpy array, convert to numpy
        if not isinstance(X_cluster, np.ndarray):
            X_cluster_values = X_cluster.to_numpy(dtype=np.float32, copy=False)
        else:
            X_cluster_values = X_cluster

        for reducer in self.reducers:
            self.logger.info(f"Reducing dimensions using {reducer.name}")
            #X_cluster = reducer.fit_transform(X_cluster)
            X_cluster = reducer.fit_transform(X_cluster_values)
            self.logger.info(f"Shape after {reducer.name}: {X_cluster_values.shape}")

        # Cluster
        self.logger.info(f"Clustering using {self.clusterer.name}")
        labels = self.clusterer.fit_predict(X_cluster_values)
        probabilities = self.clusterer.probabilities_ if hasattr(self.clusterer, "probabilities_") else None
        self.logger.info(f"Cluster labels assigned: {set(labels)}")
        if probabilities is not None:
            self.logger.info(f"Cluster probabilities shape: {probabilities.shape}")
        else:
            self.logger.info("Cluster probabilities not available for this clusterer")


        # Reduce (for visualisation)
        viz_reducer = ReducerFactory.create_reducer(name='umap', n_components=self.dimensions)
        self.logger.info(f"Reducing dimensions using {viz_reducer}")
        X_reduced = viz_reducer.fit_transform(X_cluster_values)
        self.logger.info(f"Reduced shape: {X_reduced.shape}")

        # Plot
        self.logger.info(f"Plotting clusters using {self.plotter.name}")
        fig, ax, scatter = self.plotter.plot(X_reduced, labels)
        self.logger.info("Cluster plot generated")
        plot_path = os.path.join(self.plotter.output_dir, f"{self.name}_cluster_plot.png")
        self.plotter.save(fig, plot_path)
        self.plotter.save_embeddings(X_reduced, labels, df_original, prefix=f"{self.name}_clustering_pipeline")
        self.logger.info(f"Cluster plot saved as '{self.name}_cluster_plot.png'")
        self.plotter.save_interactive_plot(X_reduced, labels, prefix=f"{self.name}_cluster_plot")
        if probabilities is not None:
            self.plotter.save_interactive_plot_by_probability(
                X_reduced,
                labels,
                probabilities,
                prefix=f"{self.name}_cluster_plot_by_probability"
            )

        # Attach cluster labels
        df_original["cluster"] = labels
        # Convert nullable Int64 to float for CSV compatibility
        for col in df_original.columns:
            if isinstance(df_original[col].dtype, pd.Int64Dtype):
                df_original[col] = df_original[col].astype(float)

        # save dataframe with cluster labels
        output_path = os.path.join(self.plotter.output_dir, f"{self.name}_clustered_data.csv")
        try:
            df_original.to_csv(output_path, index=False, encoding='utf-8', errors='replace')
            self.logger.info(f"Clustered data with labels saved to {output_path}")
        except Exception as e:
            self.logger.error(f"Error saving clustered data to {output_path}: {e}")

        # # Run evaluator if configured
        # if self.evaluator is not None and self.evaluator_metrics:
        #     try:
        #         self.logger.info(f"Running evaluator {self.evaluator.name} metrics={self.evaluator_metrics}")
        #         eval_results = self.evaluator.evaluate(df_original if hasattr(df_original, 'to_numpy') else df_original.values, labels, clusterer=self.clusterer, metrics=self.evaluator_metrics, params=self.evaluator_params)
        #         self.logger.info(f"Evaluator results: {eval_results}")
        #     except Exception as e:
        #         self.logger.error(f"Error running evaluator: {e}")

        for evaluator, cfg in self.evaluators:
            self.logger.info(f"Running evaluator {evaluator.name} with config {cfg}")
            metrics = cfg.get("metrics", [])
            params = cfg.get("params", {})

            if evaluator.name == "clustering_quality":
                self.logger.info("Evaluating clustering quality")
                evaluator.evaluate(
                    X_cluster,
                    labels,
                    clusterer=self.clusterer,
                    metrics=metrics,
                    params=params
                )

            elif evaluator.name == "cluster_profile":
                self.logger.info("Evaluating cluster profile")
                evaluator.evaluate(
                    df_original,
                    labels,
                    metrics=metrics,
                    params=params
                )

        if self.vectorizer is not None:
        # Optional: extract cluster keywords
            cluster_keywords = self._extract_cluster_keywords(df, labels)
            # Log cluster keywords
            label_counts = Counter(labels)
            for cluster_id, keywords in cluster_keywords.items():
                size = label_counts.get(cluster_id, 0)
                self.logger.info(
                    f"Cluster {cluster_id} (size={size}) keywords: {keywords}"
                )

            # save cluster keywords to file
            keyword_path = os.path.join(self.plotter.output_dir, f"{self.name}_cluster_keywords.txt")
            self._save_cluster_keywords(cluster_keywords, keyword_path)
        #self._save_cluster_keywords(cluster_keywords, "output/clustering_pipeline_cluster_keywords.txt")


        return df #, cluster_keywords

    def _save_cluster_keywords(self, cluster_keywords, filepath):
        try:
            self.logger.info(f"Saving cluster keywords to {filepath}")
            with open(filepath, "w", encoding = "utf-8", errors="replace") as f:
                for cluster_id, keywords in cluster_keywords.items():
                    f.write(f"Cluster {cluster_id} keywords: {', '.join(keywords)}\n")
            self.logger.info(f"Cluster keywords saved to {filepath}")
        except Exception as e:
            self.logger.error(f"Error saving cluster keywords to {filepath}: {e}")

    def _extract_cluster_keywords(self, X, labels, top_n=10):
        terms = self.vectorizer.get_feature_names()
        result = {}

        for cluster_id in set(labels):
            if cluster_id == -1:
                continue

            idx = np.where(labels == cluster_id)[0]
            centroid = X[idx].mean(axis=0).A1
            top_idx = centroid.argsort()[-top_n:]

            result[cluster_id] = [terms[i] for i in top_idx]

        return result
