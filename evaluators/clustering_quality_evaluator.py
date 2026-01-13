#evaluators/clustering_quality_evaluator.py
import os
import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import silhouette_score, silhouette_samples
from logs.logger import get_logger


class ClusteringQualityEvaluator:
    """
    Evaluates clustering quality in feature space.
    Metrics:
      - silhouette_average
      - silhouette_per_point
      - elbow
    """

    def __init__(self, name="clustering_quality", output_dir=".", params=None, plotter_name=None, plotter_params=None):
        self.name = name
        self.output_dir = params.get("output_dir", output_dir) if params else output_dir
        self.logger = get_logger(f"ClusteringQualityEvaluator.{name}")
        self.logger.info(f"Initialized ClusteringQualityEvaluator '{name}' with output_dir: {output_dir}")

    def _to_array(self, X):
        if hasattr(X, "toarray"):
            return X.toarray()
        if hasattr(X, "to_numpy"):
            return X.to_numpy(dtype=np.float32, copy=False)
        return np.asarray(X, dtype=np.float32)

    def _save_fig(self, fig, fname):
        path = os.path.join(self.output_dir, fname)
        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)
        self.logger.info(f"Saved {path}")

    def evaluate(self, X, labels, clusterer=None, metrics=None, params=None):
        self.logger.info("Starting clustering quality evaluation")
        metrics = metrics or []
        params = params or {}
        results = {}

        X_arr = self._to_array(X)

        # ---------------- silhouette ----------------
        if any(m.startswith("silhouette") for m in metrics):
            unique = set(labels) - {-1}
            if len(unique) < 2:
                self.logger.warning("Silhouette requires >=2 clusters.")
            else:
                if "silhouette_average" in metrics:
                    avg = float(silhouette_score(X_arr, labels))
                    results["silhouette_average"] = avg
                    self.logger.info(f"silhouette_average={avg:.4f}")

                if "silhouette_per_point" in metrics:
                    s_vals = silhouette_samples(X_arr, labels)

                    fig, ax = plt.subplots(figsize=(8, 6))
                    y_lower = 10
                    clusters = sorted(unique)

                    for i, cl in enumerate(clusters):
                        vals = np.sort(s_vals[labels == cl])
                        y_upper = y_lower + len(vals)
                        ax.fill_betweenx(
                            np.arange(y_lower, y_upper),
                            0,
                            vals,
                            alpha=0.7
                        )
                        ax.text(-0.05, y_lower + 0.5 * len(vals), str(cl))
                        y_lower = y_upper + 10

                    ax.axvline(np.mean(s_vals), linestyle="--", color="red")
                    ax.set_title("Silhouette plot")
                    ax.set_xlabel("Silhouette coefficient")
                    ax.set_yticks([])

                    self._save_fig(fig, f"{self.name}_silhouette.png")

        # ---------------- elbow ----------------
        if "elbow" in metrics and clusterer is not None:
            k_min = int(params.get("k_min", 2))
            k_max = int(params.get("k_max", 10))
            ks = range(k_min, k_max + 1)

            inertias = []
            for k in ks:
                cls = clusterer.__class__
                p = dict(getattr(clusterer, "params", {}))
                p["n_clusters"] = k

                new_clusterer = cls(name=clusterer.name, **p)
                new_clusterer.build()
                new_clusterer.model.fit(X_arr)

                inertias.append(new_clusterer.model.inertia_)

            # Compute elbow: max absolute second derivative
            inertias_arr = np.array(inertias)
            if len(inertias_arr) >= 3:
                second_deriv = np.diff(inertias_arr, n=2)
                elbow_idx = int(np.argmax(np.abs(second_deriv))) + 2  # +2 because np.diff reduces array size
                best_k = ks[elbow_idx]
            else:
                best_k = ks[0]

            fig, ax = plt.subplots(figsize=(6, 4))
            ax.plot(list(ks), inertias, marker="o")
            ax.axvline(x=best_k, color="red", linestyle="--", label=f"elbow k={best_k}")
            ax.set_xlabel("k")
            ax.set_ylabel("inertia")
            ax.set_title("Elbow method")

            self._save_fig(fig, f"{self.name}_elbow.png")

            results["ks"] = list(ks)
            results["inertias"] = inertias

        return results
