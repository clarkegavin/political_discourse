# evaluators/clustering_evaluators.py
import os
import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import silhouette_score, silhouette_samples
from visualisations.factory import VisualisationFactory
from logs.logger import get_logger

logger = get_logger("ClusteringEvaluator")


class ClusteringEvaluator:
    """Evaluator that supports silhouette metrics and elbow method.

    This evaluator expects to be instantiated by EvaluatorFactory and then called
    from pipelines with the feature matrix `X` (DataFrame or array), the
    assigned `labels` and the `clusterer` wrapper that was used to produce the
    clusters (so elbow can reinstantiate the same wrapper with different
    n_clusters values).
    """

    def __init__(self, name: str = "clustering", plotter_name: str = "cluster_plot", plotter_params: dict = None, **kwargs):
        self.name = name
        self.plotter_name = plotter_name or "cluster_plot"
        self.plotter_params = plotter_params or {}
        self.plotter = VisualisationFactory.get_visualisation(self.plotter_name, **self.plotter_params)
        self.output_dir = getattr(self.plotter, "output_dir", ".") if self.plotter else "."
        self.logger = get_logger(f"ClusteringEvaluator.{name}")

    def _to_array(self, X):
        try:
            # pandas DataFrame
            if hasattr(X, "to_numpy"):
                return X.to_numpy(dtype=np.float32, copy=False)
            # sparse
            if hasattr(X, "toarray"):
                return X.toarray()
            return np.asarray(X)
        except Exception:
            return np.asarray(X)

    def _save_fig(self, fig, fname: str):
        path = os.path.join(self.output_dir, fname)
        try:
            if self.plotter and hasattr(self.plotter, "save"):
                # many plotters accept fig and filepath
                try:
                    self.plotter.save(fig, path)
                    self.logger.info(f"Saved evaluator figure to {path} via plotter.save")
                    return
                except Exception:
                    # fall through to matplotlib save
                    pass
            fig.savefig(path, bbox_inches="tight")
            self.logger.info(f"Saved evaluator figure to {path}")
        except Exception as e:
            self.logger.error(f"Failed to save figure {path}: {e}")

    def evaluate(self, X, labels, clusterer=None, metrics=None, params: dict = None):
        """Evaluate clustering results.

        Args:
            X: features used for clustering (DataFrame/array)
            labels: cluster labels as returned by the clusterer
            clusterer: the wrapper clusterer instance used in the pipeline
            metrics: list of metric names to compute (e.g. ["silhouette_average","elbow"])
            params: extra params dict (e.g. k_min,k_max, prefix)

        Returns:
            dict of results
        """
        results = {}
        if metrics is None:
            metrics = []
        params = params or {}

        X_arr = self._to_array(X)

        # Silhouette metrics
        if any(m.startswith("silhouette") for m in metrics):
            # need at least 2 clusters (ignoring noise label -1)
            unique_labels = set(labels)
            effective = unique_labels - {-1} if -1 in unique_labels else unique_labels
            if len(effective) < 2:
                self.logger.warning("Silhouette requires >=2 clusters (ignoring -1). Skipping silhouette metrics.")
            else:
                if "silhouette_average" in metrics:
                    try:
                        avg = float(silhouette_score(X_arr, labels))
                        self.logger.info(f"silhouette_average: {avg:.6f}")
                        results["silhouette_average"] = avg
                    except Exception as e:
                        self.logger.error(f"Error computing silhouette_average: {e}")

                if "silhouette_per_point" in metrics:
                    try:
                        s_vals = silhouette_samples(X_arr, labels)
                        #results["silhouette_per_point"] = s_vals.tolist()

                        # Create silhouette plot
                        fig, ax = plt.subplots(figsize=(8, 6))
                        y_lower = 10
                        # consider clusters in sorted order ignoring -1
                        clusters = sorted(effective)
                        for i, cluster in enumerate(clusters):
                            ith_vals = s_vals[labels == cluster]
                            ith_vals.sort()
                            size_cluster = ith_vals.shape[0]
                            y_upper = y_lower + size_cluster
                            # use get_cmap to avoid linter warnings
                            cmap = plt.cm.get_cmap("nipy_spectral")
                            color = cmap(float(i) / max(len(clusters), 1))
                            ax.fill_betweenx(np.arange(y_lower, y_upper), 0, ith_vals, facecolor=color, edgecolor=color, alpha=0.7)
                            ax.text(-0.05, y_lower + 0.5 * size_cluster, str(cluster))
                            y_lower = y_upper + 10

                        ax.set_title("Silhouette plot per cluster")
                        ax.set_xlabel("Silhouette coefficient values")
                        ax.set_ylabel("Cluster label")
                        ax.axvline(x=np.mean(s_vals), color="red", linestyle="--")
                        ax.set_yticks([])
                        ax.set_xlim([-0.1, 1])

                        prefix = params.get("prefix", self.name)
                        fname = f"{prefix}_silhouette_per_point.png"
                        self._save_fig(fig, fname)
                        plt.close(fig)
                    except Exception as e:
                        self.logger.error(f"Error computing silhouette_per_point: {e}")

        # Elbow method: try to reuse the provided clusterer wrapper
        if "elbow" in metrics:
            if clusterer is None:
                self.logger.warning("Elbow metric requires the original clusterer wrapper to be passed. Skipping elbow.")
            else:
                try:
                    k_min = int(params.get("k_min", 2))
                    k_max = int(params.get("k_max", 10))
                    step = int(params.get("step", 1))
                    ks = list(range(k_min, k_max + 1, step))
                    inertias = []

                    # Prepare X array for fitting
                    # Some clusterer wrappers expect DataFrame and convert internally; we will pass numpy
                    X_fit = X_arr

                    for k in ks:
                        # create new wrapper instance of same class with same params
                        cls = clusterer.__class__
                        # copy params if present
                        original_params = getattr(clusterer, "params", {})
                        params_copy = dict(original_params) if isinstance(original_params, dict) else {}
                        params_copy["n_clusters"] = k
                        new_clusterer = cls(name=clusterer.name, **params_copy)

                        # build and fit underlying sklearn estimator
                        if hasattr(new_clusterer, "build"):
                            new_clusterer.build()
                        # if the wrapper exposes .model, fit directly
                        if hasattr(new_clusterer, "model") and new_clusterer.model is not None:
                            new_clusterer.model.fit(X_fit)
                            inertia = getattr(new_clusterer.model, "inertia_", None)
                        else:
                            # fallback: call fit on wrapper and then inspect model
                            new_clusterer.fit(X_fit)
                            inertia = getattr(new_clusterer, "model", None)
                            inertia = getattr(inertia, "inertia_", None) if inertia is not None else None

                        inertias.append(float(inertia) if inertia is not None else None)

                    # compute elbow via max absolute second derivative where defined
                    inertias_arr = np.array([i for i in inertias if i is not None])
                    if inertias_arr.size >= 3:
                        second_deriv = np.diff(inertias_arr, n=2)
                        if second_deriv.size > 0:
                            elbow_idx = int(np.argmax(np.abs(second_deriv))) + 2
                            best_k = ks[elbow_idx]
                        else:
                            best_k = ks[0]
                    else:
                        best_k = ks[0]

                    results["elbow_k"] = int(best_k)
                    results["ks"] = ks
                    results["inertias"] = inertias

                    # plot inertia vs k
                    fig, ax = plt.subplots(figsize=(6, 4))
                    ax.plot(ks, inertias, marker="o")
                    ax.set_xlabel("k")
                    ax.set_ylabel("inertia")
                    ax.set_title("Elbow method: inertia vs k")
                    ax.axvline(x=best_k, color="red", linestyle="--", label=f"elbow k={best_k}")
                    ax.legend()
                    prefix = params.get("prefix", self.name)
                    fname = f"{prefix}_elbow.png"
                    self._save_fig(fig, fname)
                    plt.close(fig)

                    self.logger.info(f"Elbow method selected k={best_k} (ks={ks})")
                except Exception as e:
                    self.logger.error(f"Error computing elbow method: {e}")

        # Descriptive statistics and per-cluster visualisations
        if "descriptive_stats" in metrics:
            try:
                # Descriptive stats require a pandas DataFrame with columns
                import pandas as pd

                # Determine whether original X is a DataFrame
                df = None
                if hasattr(X, "columns") and hasattr(X, "loc"):
                    df = X.copy()
                else:
                    # if we can't obtain a DataFrame, try params to find one
                    df = params.get("df") if isinstance(params.get("df"), pd.DataFrame) else None

                if df is None:
                    self.logger.warning("Descriptive stats require a pandas DataFrame input; skipping descriptive statistics.")
                else:
                    # attach cluster labels
                    df_with_labels = df.copy()
                    df_with_labels["cluster"] = labels

                    # columns configuration: allow user to pass lists or auto-detect
                    numeric_cols = params.get("numeric_columns") or params.get("numeric")
                    cat_cols = params.get("categorical_columns") or params.get("categorical")

                    if numeric_cols is None:
                        #exclude sparse numeric columns
                        #numeric_cols = df_with_labels.select_dtypes(include=[np.number]).columns.tolist()
                        # numeric_cols = [
                        #     c for c in df_with_labels.select_dtypes(include=[np.number]).columns
                        #     if not isinstance(df_with_labels[c].dtype, pd.SparseDtype)
                        # ]

                        numeric_cols = []
                        binary_sparse_cols = []

                        for col in df_with_labels.columns:
                            if col == "cluster":
                                continue

                            dtype = df_with_labels[col].dtype

                            # sparse binary indicators (multi-hot)
                            if isinstance(dtype, pd.SparseDtype):
                                binary_sparse_cols.append(col)

                            # true numeric (continuous)
                            elif np.issubdtype(dtype, np.number):
                                numeric_cols.append(col)

                    if cat_cols is None:
                        cat_cols = df_with_labels.select_dtypes(include=[object, "category"]).columns.tolist()

                    # Remove the cluster column if present in lists
                    numeric_cols = [c for c in numeric_cols if c != "cluster"]
                    cat_cols = [c for c in cat_cols if c != "cluster"]

                    descriptive = {"numeric_summary": {}, "categorical_summary": {}}

                    clusters_sorted = sorted(list(set(labels)))

                    # Numeric summaries
                    for col in numeric_cols:
                        try:
                            grp = df_with_labels.groupby("cluster")[col]
                            summary = grp.agg(["count", "mean", "median", "std", "min", "max"]).to_dict()
                            # convert nested dicts to simple mappings
                            # pandas returns {stat: {cluster: value}}; reorient to {cluster: {stat: value}}
                            per_cluster = {}
                            for stat, vals in summary.items():
                                for cl, v in vals.items():
                                    per_cluster.setdefault(str(cl), {})[stat] = float(v) if pd.notnull(v) else None
                            descriptive["numeric_summary"][col] = per_cluster

                            # Create boxplot comparing clusters for this numeric column
                            data_list = [df_with_labels.loc[df_with_labels["cluster"] == cl, col].dropna().values for cl in clusters_sorted]
                            # if all data_list are empty, skip
                            if any(len(arr) > 0 for arr in data_list):
                                fig, ax = plt.subplots(figsize=(8, 6))
                                ax.boxplot(data_list, labels=[str(c) for c in clusters_sorted], patch_artist=True)
                                ax.set_title(f"{col} by cluster")
                                ax.set_xlabel("cluster")
                                ax.set_ylabel(col)
                                # Save using VisualisationFactory's BoxPlot visualiser if available
                                box_viz = VisualisationFactory.get_visualisation("boxplot", title=f"{col} by cluster", figsize=(8, 6), ylabel=col)
                                prefix = params.get("prefix", self.name)
                                fname = f"{prefix}_{col}_boxplot_by_cluster.png"
                                if box_viz is not None and hasattr(box_viz, "save"):
                                    # use visualiser to save for consistent output_dir handling
                                    box_viz.save(fig, os.path.join(getattr(box_viz, 'output_dir', self.output_dir), fname))
                                else:
                                    self._save_fig(fig, fname)
                                plt.close(fig)
                        except Exception as e:
                            self.logger.error(f"Error computing numeric descriptive for {col}: {e}")

                    descriptive["binary_sparse_summary"] = {}

                    for col in binary_sparse_cols:
                        try:
                            # Convert sparse to dense *per column only* (cheap)
                            col_dense = df_with_labels[col].sparse.to_dense()

                            # Proportion of games in each cluster with this feature
                            proportions = (
                                col_dense
                                .groupby(df_with_labels["cluster"])
                                .mean()
                            )

                            descriptive["binary_sparse_summary"][col] = {
                                str(cl): float(val)
                                for cl, val in proportions.items()
                            }
                            # # Optional: bar chart
                            # fig, ax = plt.subplots(figsize=(6, 4))
                            # ax.bar(
                            #     [str(c) for c in proportions.index],
                            #     proportions.values
                            # )
                            # ax.set_title(f"{col} prevalence by cluster")
                            # ax.set_xlabel("cluster")
                            # ax.set_ylabel("proportion")
                            #
                            # prefix = params.get("prefix", self.name)
                            # fname = f"{prefix}_{col}_proportion_by_cluster.png"
                            # self._save_fig(fig, fname)
                            # plt.close(fig)

                        except Exception as e:
                            self.logger.error(f"Error computing binary sparse descriptive for {col}: {e}")


                    # Categorical summaries
                    top_n = int(params.get("top_n", 10))
                    for col in cat_cols:
                        try:
                            # compute value counts per cluster
                            counts = df_with_labels.groupby(["cluster", col]).size().unstack(fill_value=0)
                            # limit categories to top_n by overall frequency
                            total_counts = counts.sum(axis=0).sort_values(ascending=False)
                            top_categories = list(total_counts.head(top_n).index)
                            counts = counts[top_categories]

                            # convert to nested dict: {cluster: {category: count}}
                            counts_dict = {}
                            for cl in counts.index:
                                counts_dict[str(cl)] = {str(cat): int(counts.loc[cl, cat]) for cat in counts.columns}
                            descriptive["categorical_summary"][col] = counts_dict

                            # Create grouped barplot: categories on x, bars per cluster
                            fig, ax = plt.subplots(figsize=(10, 6))
                            categories = counts.columns.tolist()
                            n_categories = len(categories)
                            n_clusters = len(counts.index)
                            x = np.arange(n_categories)
                            total_width = 0.8
                            width = total_width / max(n_clusters, 1)
                            for i, cl in enumerate(counts.index):
                                vals = counts.loc[cl].values.astype(float)
                                ax.bar(x + i * width, vals, width=width, label=str(cl))
                            ax.set_xticks(x + (n_clusters - 1) * width / 2)
                            ax.set_xticklabels([str(c) for c in categories], rotation=params.get("xticks_rotation", 45))
                            ax.set_title(f"{col} category counts by cluster")
                            ax.set_xlabel(col)
                            ax.set_ylabel("count")
                            ax.legend(title="cluster")
                            # Save using VisualisationFactory's BarChart visualiser if available
                            bar_viz = VisualisationFactory.get_visualisation("bar_chart", title=f"{col} category counts by cluster", figsize=(10, 6), xlabel=col, ylabel="count")
                            prefix = params.get("prefix", self.name)
                            fname = f"{prefix}_{col}_bar_by_cluster.png"
                            if bar_viz is not None and hasattr(bar_viz, "save"):
                                bar_viz.save(fig, os.path.join(getattr(bar_viz, 'output_dir', self.output_dir), fname))
                            else:
                                self._save_fig(fig, fname)
                            plt.close(fig)
                        except Exception as e:
                            self.logger.error(f"Error computing categorical descriptive for {col}: {e}")

                    results["descriptive_stats"] = descriptive
            except Exception as e:
                self.logger.error(f"Error computing descriptive statistics: {e}")

        return results
