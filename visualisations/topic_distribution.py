import matplotlib.pyplot as plt
import os

class TopicDistribution:

    def __init__(self, output_dir=None, filename="topic_distribution.png", top_n=10):
        self.output_dir = output_dir
        self.filename = filename
        self.top_n = top_n
        self.topic_id = None  # Store topic_id

    def plot(self, df, **kwargs):
        #get topic id from kwargs if provided, if not default to "_topic_id"
        self.topic_id = kwargs.get("topic_id", "_topic_id")

        df_filtered = df[df[self.topic_id] != -1]

        topic_counts = df_filtered[self.topic_id].value_counts().head(self.top_n)

        # Map labels if available
        if "topic_label" in df.columns:
            labels = (
                df_filtered.groupby(self.topic_id)["topic_label"]
                .first()
            )
            topic_counts.index = topic_counts.index.map(labels)

        fig = plt.figure()
        topic_counts.plot(kind="bar")

        #plt.title(f"Top {self.top_n} Topics by Document Count")
        plt.xlabel("Topic")
        plt.ylabel("Number of Documents")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()

        return fig

    def save(self, fig, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fig.savefig(path)
        plt.close(fig)