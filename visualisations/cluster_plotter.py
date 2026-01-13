# visualisations/cluster_plotter.py
import matplotlib.pyplot as plt
from .base import Visualisation
from logs.logger import get_logger
import os
import pandas as pd
from collections import Counter
import numpy as np
import plotly.express as px
import matplotlib.patches as mpatches
import matplotlib.cm as cm
import matplotlib.colors as mcolors

class ClusterPlotter(Visualisation):
    """
    Cluster scatter plot visualisation.
    """

    def __init__(self, name: str = "cluster_plot",  title="Cluster Visualisation",
                 output_dir = ".", xlabel=None, ylabel=None, zlabel=None, figsize=(10, 6), **params):
        super().__init__(title=title, figsize=figsize)
        self.logger = get_logger(self.__class__.__name__)
        self.name = name
        self.title = title
        self.xlabel = xlabel
        self.ylabel = ylabel
        self.zlabel = zlabel
        self.figsize = figsize
        self.params = params  # optional style parameters
        self.output_dir = output_dir
        self.logger.info(
            f"Initialized ClusterPlotter with title={title}, xlabel={xlabel}, ylabel={ylabel}, figsize={figsize}, params={params}"
        )

    def build(self):
        self.logger.info(f"Built ClusterPlotter with params: {self.params}")
        return self


    def plot(self, data, labels=None, **kwargs):
        """
        Plot 2D or 3D cluster scatter plot.
        Automatically detects dimensionality.

        Parameters:
            X_reduced: np.ndarray of shape (n_samples, 2 or 3)
            labels: cluster labels for each sample
            kwargs: optional style overrides (e.g. cmap, alpha)
        """
        # Convert DataFrame to NumPy array if necessary
        # Accept either a DataFrame or array-like in `data`
        if isinstance(data, pd.DataFrame):
            X_plot = data.values
        else:
            X_plot = data

        n_dims = X_plot.shape[1]
        self.logger.info(f"Creating cluster plot with {X_plot.shape[0]} points and {n_dims}D embedding")

        # Prepare label -> color mapping (handle categorical labels)
        cmap_name = kwargs.pop('cmap', self.params.get('cmap', 'tab10'))
        color_vals = None
        legend_labels = None
        if labels is not None:
            labels_arr = np.asarray(labels)
            if not np.issubdtype(labels_arr.dtype, np.number):
                uniques, inverse = np.unique(labels_arr, return_inverse=True)
                color_vals = inverse
                legend_labels = list(uniques)
            else:
                color_vals = labels_arr

        # --- 3D PLOT ---------------------------------------------------------
        if n_dims == 3:
            fig = plt.figure(figsize=self.figsize)
            ax = fig.add_subplot(111, projection="3d")

            scatter = ax.scatter(
                X_plot[:, 0],
                X_plot[:, 1],
                X_plot[:, 2],
                c=color_vals if color_vals is not None else labels,
                cmap=cmap_name,
                **kwargs
            )

            ax.set_title(self.title)
            if self.xlabel:
                ax.set_xlabel(self.xlabel)
            if self.ylabel:
                ax.set_ylabel(self.ylabel)
            zlabel = self.params.get("zlabel")
            if zlabel:
                ax.set_zlabel(zlabel)

            self.logger.info("3D cluster plot created")
            return fig, ax, scatter

        # --- 2D PLOT ---------------------------------------------------------
        elif n_dims == 2:
            fig, ax = plt.subplots(figsize=self.figsize)
            scatter = ax.scatter(
                X_plot[:, 0],
                X_plot[:, 1],
                c=color_vals if color_vals is not None else labels,
                cmap=cmap_name,
                **kwargs
            )

            ax.set_title(self.title)

            if self.xlabel:
                ax.set_xlabel(self.xlabel)
            if self.ylabel:
                ax.set_ylabel(self.ylabel)

            # Optional: xticks rotation
            rotation = self.params.get("xticks_rotation")
            if rotation is not None:
                ax.tick_params(axis='x', labelrotation=rotation)

            # Optional: label large clusters
            min_size = self.params.get("label_min_cluster_size", 100)
            self._label_large_clusters(ax, X_plot, labels, min_cluster_size=min_size)

            # Add legend for categorical labels if present
            if legend_labels is not None:
                try:
                    cmap_obj = cm.get_cmap(cmap_name)
                except Exception:
                    cmap_obj = cm.get_cmap('tab10')
                patches = []
                for i, lab in enumerate(legend_labels):
                    # Normalize index into colormap
                    color = mcolors.to_hex(cmap_obj(i / max(1, len(legend_labels)-1)))
                    patches.append(mpatches.Patch(color=color, label=str(lab)))
                ax.legend(handles=patches, title='label')

            self.logger.info("2D cluster plot created with cluster labels (if enabled)")
            return fig, ax, scatter

        else:
            raise ValueError(f"Can only plot 2D or 3D, got {n_dims} dimensions")

    def save_embeddings(self, X_embedded, labels, df_original, prefix="embedding"):
        """
        Save reduced coordinates + cluster labels + original metadata.
        """
        self.logger.info(f"Saving embeddings with prefix '{prefix}'")

        # Convert array → dataframe
        if X_embedded.shape[1] == 2:
            reduced_df = pd.DataFrame(X_embedded, columns=["x", "y"])
        elif X_embedded.shape[1] == 3:
            reduced_df = pd.DataFrame(X_embedded, columns=["x", "y", "z"])
        else:
            raise ValueError("X_embedded must be 2D or 3D for plotting.")

        # Add cluster labels
        reduced_df["cluster"] = labels

        # Add metadata from the original df (optional)
        # meta_cols = ["gameID", "Name", "Genre"]
        # for col in meta_cols:
        #     if col in df_original.columns:
        #         reduced_df[col] = df_original[col].values

        # Save to CSV
        # create output directory if it doesn't exist
        self.logger.info(f"Creating output directory at {self.output_dir} if it doesn't exist")
        os.makedirs(self.output_dir, exist_ok=True)
        out_path = os.path.join(self.output_dir, f"{prefix}.csv")
        reduced_df.to_csv(out_path, index=False)

        self.logger.info(f"Saved embedding CSV to {self.output_dir}")
        return out_path

    def _label_large_clusters(self, ax, X_reduced, labels, min_cluster_size=100):
        """
        Annotate only the largest clusters on the scatter plot.

        Parameters:
            ax : matplotlib axes object
            X_reduced : np.ndarray (n_samples, 2)
            labels : np.ndarray of cluster labels
            min_cluster_size : int
               Minimum samples required for a cluster to be labeled
        """
        counts = Counter(labels)
        large_clusters = {c: n for c, n in counts.items() if c != -1 and n >= min_cluster_size}

        self.logger.info(f"Labeling {len(large_clusters)} clusters (min size={min_cluster_size})")

        for c in large_clusters:
            # centroid in UMAP/embedding space
            centroid = np.mean(X_reduced[labels == c], axis=0)

            ax.text(
                centroid[0],
                centroid[1],
                f"Cluster {c}",
                fontsize=10,
                fontweight="bold",
                bbox=dict(facecolor="white", alpha=0.7, edgecolor="black", pad=2)
            )

    def save_interactive_plot(self, X_reduced, labels, prefix="cluster_plot"):
        """
        Save interactive 2D or 3D cluster plot as HTML (Plotly).
        """
        self.logger.info("Saving interactive Plotly cluster plot")

        os.makedirs(self.output_dir, exist_ok=True)
        # Convert DataFrame to NumPy array if necessary
        if isinstance(X_reduced, pd.DataFrame):
            X_plot = X_reduced.values
        else:
            X_plot = X_reduced

        # Build dataframe
        if X_plot.shape[1] == 2:
            df = pd.DataFrame({
                "x": X_plot[:, 0],
                "y": X_plot[:, 1],
                "cluster": labels
            })

            fig = px.scatter(
                df,
                x="x",
                y="y",
                color="cluster",
                title=self.title
            )

        elif X_plot.shape[1] == 3:
            df = pd.DataFrame({
                "x": X_plot[:, 0],
                "y": X_plot[:, 1],
                "z": X_plot[:, 2],
                "cluster": labels
            })

            fig = px.scatter_3d(
                df,
                x="x",
                y="y",
                z="z",
                color="cluster",
                title=self.title + " By Cluster"
            )

        else:
            raise ValueError("Only 2D or 3D embeddings supported")

        out_path = os.path.join(self.output_dir, f"{prefix}.html")
        fig.write_html(out_path)

        self.logger.info(f"Interactive plot saved to {out_path}")
        return out_path

    def save_interactive_plot_by_probability(self, X_reduced, labels, probabilities, prefix="cluster_plot"):
        """
        Save interactive 2D or 3D cluster plot as HTML (Plotly).
        """
        self.logger.info("Saving interactive Plotly cluster plot")

        os.makedirs(self.output_dir, exist_ok=True)

        # Convert DataFrame to NumPy array if necessary
        if isinstance(X_reduced, pd.DataFrame):
            X_plot = X_reduced.values
        else:
            X_plot = X_reduced

        # Build dataframe
        if X_plot.shape[1] == 2:
            df = pd.DataFrame({
                "x": X_plot[:, 0],
                "y": X_plot[:, 1],
                "cluster": labels
            })

            fig = px.scatter(
                df,
                x="x",
                y="y",
                color="cluster",
                title=self.title
            )

        elif X_plot.shape[1] == 3:
            df = pd.DataFrame({
                "x": X_plot[:, 0],
                "y": X_plot[:, 1],
                "z": X_plot[:, 2],
                "cluster": labels
            })

            fig = px.scatter_3d(
                df,
                x="x",
                y="y",
                z="z",
                color="cluster",
                hover_data={'cluster': True},
                #color="probability",
                # color_continuous_scale='Viridis',
                # hover_data={'cluster': True, 'probability': ':.4f', 'is_noise': True},
                title=self.title + f" By Cluster"
            )

        else:
            raise ValueError("Only 2D or 3D embeddings supported")

            # Improve layout for academic clarity
        fig.update_layout(
            coloraxis_colorbar=dict(
                title="Cluster"
            )
        )
        out_path = os.path.join(self.output_dir, f"{prefix}.html")
        fig.write_html(out_path)

        self.logger.info(f"Interactive plot saved to {out_path}")
        return out_path
