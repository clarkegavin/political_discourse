import pandas as pd
import os

class EmbeddingLoader:
    """
    Helper class to load saved embedding CSV files produced by ClusterPlotter.
    """

    @staticmethod
    def load(path: str):
        """
        Load an embeddings CSV and return:
            - coords: numpy array of shape (n_samples, 2 or 3)
            - labels: numpy array of shape (n_samples,)
            - metadata: dataframe containing the rest of the columns
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"Embedding file not found: {path}")

        df = pd.read_csv(path)

        # Determine coordinate columns automatically
        coord_cols = [col for col in df.columns if col in ("x", "y", "z")]
        if len(coord_cols) not in (2, 3):
            raise ValueError(
                f"Invalid embedding file. Expected 2 or 3 coordinate columns, got {coord_cols}"
            )

        # Extract
        coords = df[coord_cols].values
        labels = df["cluster"].values if "cluster" in df.columns else None

        # Everything else is metadata
        meta_cols = [col for col in df.columns if col not in coord_cols + ["cluster"]]
        metadata = df[meta_cols].copy()

        return coords, labels, metadata

    @staticmethod
    def load_as_dataframe(path: str):
        """
        Load the entire embedding CSV as a pandas dataframe.
        Useful for debugging or direct manipulation.
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"Embedding file not found: {path}")

        return pd.read_csv(path)

# Example usage:
# coords, labels, metadata = EmbeddingLoader.load("output/embedding.csv")
#import matplotlib.pyplot as plt

#plt.scatter(coords[:,0], coords[:,1], c=labels)
#plt.show()
