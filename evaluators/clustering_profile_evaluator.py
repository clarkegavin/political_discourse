#evaluators/clustering_profile_evaluator.py
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from logs.logger import get_logger


class ClusterProfileEvaluator:
    """
    Profiles clusters using original dataframe.
    """

    def __init__(self, name="cluster_profile", output_dir=".", params=None, plotter_name=None, plotter_params=None):
        self.name = name
        self.output_dir = params.get("output_dir", output_dir) if params else output_dir
        self.logger = get_logger(f"ClusterProfileEvaluator.{name}")
        self.logger.info(f"Initialized ClusterProfileEvaluator '{name}' with output_dir: {output_dir}")

    def _save_fig(self, fig, fname):
        path = os.path.join(self.output_dir, fname)
        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)
        self.logger.info(f"Saved {path}")

    def evaluate(self, df, labels, metrics=None, params=None):
        self.logger.info("Starting cluster profiling evaluation")
        metrics = metrics or []
        params = params or {}
        results = {}

        df = df.copy()
        df["cluster"] = labels

        # ---- success metric ----
        required_cols = {"Total_Reviews", "Review_Score"}
        if required_cols.issubset(df.columns):
            df["success_index"] = np.log1p(df["Total_Reviews"]) * df["Review_Score"]
        else:
            self.logger.warning(
                "Missing columns for success_index; skipping success metric"
            )
            df["success_index"] = np.nan

        # ---------------- numeric ----------------
        # exclude sparse numeric columns as they are likely binary indicators and handled separately
        numeric_cols = [
            c for c in df.select_dtypes(include=[np.number]).columns
            if c != "cluster" and not isinstance(df[c].dtype, pd.SparseDtype)
        ]

        results["numeric_summary"] = {}

        for col in numeric_cols:
            grp = df.groupby("cluster")[col]
            stats = grp.agg(["count", "mean", "median", "std", "min", "max"])
            results["numeric_summary"][col] = stats.to_dict()

            fig, ax = plt.subplots(figsize=(8, 6))
            data = [df[df.cluster == c][col].dropna() for c in sorted(df.cluster.unique())]
            ax.boxplot(data, labels=sorted(df.cluster.unique()))
            ax.set_title(f"{col} by cluster")

            self._save_fig(fig, f"{self.name}_{col}_boxplot.png")

        # ---------------- sparse binary ----------------
        sparse_cols = [
            c for c in df.columns
            if isinstance(df[c].dtype, pd.SparseDtype)
        ]

        results["binary_sparse_summary"] = {}

        for col in sparse_cols:
            dense = df[col].sparse.to_dense()
            proportions = dense.groupby(df["cluster"]).mean()
            results["binary_sparse_summary"][col] = proportions.to_dict()

        # ---------------- categorical ----------------
        cat_cols = df.select_dtypes(include=["object", "category"]).columns
        cat_cols = [c for c in cat_cols if c != "cluster"]

        results["categorical_summary"] = {}

        top_n = int(params.get("top_n", 10))

        for col in cat_cols:
            counts = df.groupby(["cluster", col]).size().unstack(fill_value=0)
            top = counts.sum().sort_values(ascending=False).head(top_n).index
            results["categorical_summary"][col] = counts[top].to_dict()

        # ---------------- success summary ----------------
        results["success_summary"] = {}
        self.logger.info("Generating success index summary by cluster")
        if "success_index" in df.columns:
            grp = df.groupby("cluster")["success_index"]

            summary = grp.agg(
                count="count",
                mean="mean",
                median="median",
                std="std",
                min="min",
                max="max",
            )

            results["success_summary"] = summary.to_dict()

            # Boxplot
            fig, ax = plt.subplots(figsize=(8, 6))
            data = [
                df[df.cluster == c]["success_index"].dropna()
                for c in sorted(df.cluster.unique())
            ]
            ax.boxplot(data, labels=sorted(df.cluster.unique()))
            ax.set_title("Success Index by Cluster")
            ax.set_ylabel("Success Index")

            self._save_fig(fig, f"{self.name}_success_index_boxplot.png")
            self.logger.info("Generated success index summary by cluster")

        # Persist CSVs for  interpretation
        self.logger.info("Saving cluster profile summaries to CSV")
        self._save_numeric_csv(results["numeric_summary"])
        self._save_binary_csv(results["binary_sparse_summary"])
        self._save_categorical_csv(results["categorical_summary"])

        self.logger.info("Saved cluster profile summaries to CSV")
        if "success_index" in df.columns:
            self.logger.info("Saving cluster success ranking to CSV")
            self._save_cluster_ranking(df)

        return results

    def _save_numeric_csv(self, numeric_summary):
        rows = []
        for feature, stats in numeric_summary.items():
            for stat, cluster_vals in stats.items():
                for cluster, value in cluster_vals.items():
                    rows.append({
                        "feature": feature,
                        "cluster": cluster,
                        "statistic": stat,
                        "value": value
                    })

        df = pd.DataFrame(rows)
        self.logger.info(f"Saving numeric summary data to {self.output_dir}")
        path = os.path.join(self.output_dir, f"{self.name}_numeric_summary.csv")
        df.to_csv(path, index=False)
        self.logger.info(f"Saved numeric summary data to {path}")

    def _save_binary_csv(self, binary_summary):
        rows = []
        for feature, cluster_vals in binary_summary.items():
            for cluster, proportion in cluster_vals.items():
                rows.append({
                    "feature": feature,
                    "cluster": cluster,
                    "proportion": proportion
                })

        df = pd.DataFrame(rows)
        path = os.path.join(self.output_dir, f"{self.name}_binary_summary.csv")
        df.to_csv(path, index=False)
        self.logger.info(f"Saved binary summary data to {path}")

    def _save_categorical_csv(self, categorical_summary):
        rows = []
        for feature, cluster_data in categorical_summary.items():
            for cluster, categories in cluster_data.items():
                for category, count in categories.items():
                    rows.append({
                        "feature": feature,
                        "cluster": cluster,
                        "category": category,
                        "count": count
                    })

        df = pd.DataFrame(rows)
        path = os.path.join(self.output_dir, f"{self.name}_categorical_summary.csv")
        df.to_csv(path, index=False)
        self.logger.info(f"Saved categorical summary data to {path}")

    def _save_cluster_ranking(self, df):
        self.logger.info("Computing cluster success ranking")
        ranking = (
            df.groupby("cluster")["success_index"]
            .agg(
                n_games="count",
                median_success="median",
                mean_success="mean",
                top_25_pct=lambda x: x.quantile(0.75),
            )
            .sort_values("median_success", ascending=False)
            .reset_index()
        )

        ranking["rank"] = range(1, len(ranking) + 1)

        path = os.path.join(self.output_dir, f"{self.name}_cluster_success_ranking.csv")
        ranking.to_csv(path, index=False)

        self.logger.info(f"Saved cluster success ranking to {path}")