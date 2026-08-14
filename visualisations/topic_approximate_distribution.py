from .bertopic_visualisation import BERTopicVisualisation

import numpy as np
import plotly.graph_objects as go


class ApproximateDistribution(BERTopicVisualisation):
    """
    BERTopic approximate topic distribution visualisation.

    Uses BERTopic's approximate_distribution() method to calculate
    topic distributions for each document and visualises the
    aggregated distribution across the corpus.

    Parameters
    ----------
    top_n_topics : int, optional
        Number of highest-distribution topics to display.
        Set to None to display all topics.

    text_column : str
        Name of the dataframe column containing the documents.

    **params
        Parameters passed directly to BERTopic's
        approximate_distribution() method.
    """

    def __init__(
        self,
        output_dir=".",
        filename="approximate_distribution.html",
        top_n_topics=20,
        text_column="RepresentationDocument",
        **params
    ):

        super().__init__(
            name="approximate_distribution",
            title="Approximate Topic Distribution",
            output_dir=output_dir,
            filename=filename,
            method="approximate_distribution",
            output_type="figure",
            **params
        )

        self.top_n_topics = top_n_topics
        self.text_column = text_column


    def plot(
        self,
        data,
        metadata=None,
        model=None,
        save_path=None,
        filename=None,
        **plot_kwargs
    ):

        bertopic_model = self._get_bertopic_model(model)

        if bertopic_model is None:
            return None


        try:

            # --------------------------------------------------
            # Extract documents
            # --------------------------------------------------

            if hasattr(data, "columns"):

                if self.text_column not in data.columns:

                    self.logger.warning(
                        f"Document column '{self.text_column}' "
                        f"not found in dataframe. "
                        f"Available columns: "
                        f"{list(data.columns)}"
                    )

                    return None


                docs = (
                    data[self.text_column]
                    .fillna("")
                    .astype(str)
                    .tolist()
                )

            elif isinstance(data, (list, tuple)):

                docs = [
                    str(doc)
                    for doc in data
                ]

            else:

                self.logger.warning(
                    "Expected data to be either a dataframe "
                    "or a list/tuple of document strings"
                )

                return None


            self.logger.info(
                f"Computing approximate topic distributions "
                f"for {len(docs):,} documents"
            )


            # --------------------------------------------------
            # Compute approximate topic distributions
            # --------------------------------------------------

            topic_distributions, _ = (
                bertopic_model.approximate_distribution(
                    docs,
                    **self.params
                )
            )


            topic_distributions = np.asarray(
                topic_distributions
            )

            self.logger.info(
                f"Distribution min={topic_distributions.min():.6f}, "
                f"max={topic_distributions.max():.6f}, "
                f"nonzero={np.count_nonzero(topic_distributions):,}"
            )

            self.logger.info(
                f"Approximate topic distribution shape: "
                f"{topic_distributions.shape}"
            )


            if topic_distributions.ndim != 2:

                self.logger.warning(
                    "Expected approximate topic distributions "
                    "to be a 2-dimensional array"
                )

                return None


            # --------------------------------------------------
            # Determine actual BERTopic topic IDs
            # --------------------------------------------------

            topic_ids = sorted(
                topic
                for topic in bertopic_model.topic_representations_
                if topic != -1
            )


            # --------------------------------------------------
            # Validate topic mapping
            # --------------------------------------------------

            n_distribution_topics = (
                topic_distributions.shape[1]
            )

            if len(topic_ids) != n_distribution_topics:

                self.logger.warning(
                    "Mismatch between BERTopic topic IDs and "
                    "approximate distribution columns: "
                    f"{len(topic_ids)} topic IDs vs "
                    f"{n_distribution_topics} distribution columns"
                )

                return None


            self.logger.info(
                f"Mapped {len(topic_ids)} approximate "
                "distribution columns to BERTopic topic IDs"
            )


            # --------------------------------------------------
            # Aggregate distributions
            # --------------------------------------------------

            topic_totals = (
                topic_distributions.sum(axis=0)
            )


            total = topic_totals.sum()

            if total <= 0:

                self.logger.warning(
                    "Approximate topic distributions sum to zero"
                )

                return None


            topic_proportions = (
                topic_totals / total
            )


            # --------------------------------------------------
            # Sort topics by distribution
            # --------------------------------------------------

            sorted_indices = np.argsort(
                topic_proportions
            )[::-1]


            topic_ids = [
                topic_ids[index]
                for index in sorted_indices
            ]


            topic_proportions = [
                topic_proportions[index]
                for index in sorted_indices
            ]


            # --------------------------------------------------
            # Limit to top N topics
            # --------------------------------------------------

            if self.top_n_topics is not None:

                if self.top_n_topics <= 0:

                    self.logger.warning(
                        "top_n_topics must be greater than zero "
                        "or None"
                    )

                    return None


                topic_ids = (
                    topic_ids[:self.top_n_topics]
                )

                topic_proportions = (
                    topic_proportions[:self.top_n_topics]
                )


            self.logger.info(
                f"Displaying {len(topic_ids)} topics "
                "from approximate distribution"
            )


            # --------------------------------------------------
            # Create Plotly visualisation
            # --------------------------------------------------

            fig = go.Figure(
                data=[
                    go.Bar(
                        x=topic_proportions[::-1],
                        y=[
                            str(topic_id)
                            for topic_id in topic_ids[::-1]
                        ],
                        orientation="h"
                    )
                ]
            )


            fig.update_layout(
                title=self.title,
                xaxis_title="Approximate Topic Distribution",
                yaxis_title="Topic",
                **plot_kwargs
            )


        except Exception as e:

            self.logger.exception(
                f"Failed generating {self.name}: {e}"
            )

            return None


        return self._save(
            fig,
            save_path,
            filename
        )