from wordcloud import WordCloud
import matplotlib.pyplot as plt
import os
from logs.logger import get_logger


class TopicWordClouds:

    def __init__(self, output_dir=None, top_n_topics=10, filename_prefix="topic_wc"):
        self.output_dir = output_dir
        self.top_n_topics = top_n_topics
        self.filename_prefix = filename_prefix
        self.logger = get_logger(self.__class__.__name__)
        self.topic_id = None  # Store topic_id

    def plot(self, df, model=None, **kwargs):
        """
        df: result dataframe with 'topic' column
        model: BERTopic wrapper (REQUIRED)
        """
        self.logger.info("Generating topic word clouds")
        # extract topic_id from kwargs if provided, if not default to "_topic_id"
        self.topic_id = kwargs.get("topic_id", "_topic_id")


        if model is None:
            raise ValueError("TopicWordClouds requires model to generate word clouds")


        os.makedirs(self.output_dir, exist_ok=True)
        self.logger.debug(f"Output directory: {self.output_dir}")


        # Get top N topics by frequency (excluding -1
        topic_counts = (
            df[df[self.topic_id] != -1][self.topic_id]
            .value_counts()
            .head(self.top_n_topics)
        )
        self.logger.info(f"Top {self.top_n_topics} topics: {topic_counts.index.tolist()}")

        saved_paths = []

        for topic_id in topic_counts.index:
            self.logger.info(f"Generating word cloud for topic {topic_id}")
            words = model.get_topic(topic_id)

            if not words:
                continue

            # Use weights (correct way)
            freq_dict = {word: weight for word, weight in words}

            self.logger.debug(f"Topic {topic_id} word frequencies: {freq_dict}")
            wc = WordCloud(width=800, height=400)
            wc.generate_from_frequencies(freq_dict)
            self.logger.debug(f"Generated word cloud for topic {topic_id} with {len(freq_dict)} words")

            plt.figure(figsize=(10, 5))
            plt.imshow(wc, interpolation="bilinear")
            plt.axis("off")
            plt.title(f"Topic {topic_id}")
            plt.tight_layout()

            self.logger.info(f"Saving word cloud for topic {topic_id}")
            filename = f"{self.filename_prefix}_{topic_id}.png"
            path = os.path.join(self.output_dir, filename)

            plt.savefig(path)
            self.logger.info(f"Saved word cloud for topic {topic_id} to {path}")
            plt.close()

            saved_paths.append(path)

        return saved_paths  # return list instead of fig

    def save(self, paths, *_):
        # No-op because files are already saved in plot()
        return paths