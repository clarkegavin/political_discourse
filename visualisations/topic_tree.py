from .bertopic_visualisation import BERTopicVisualisation


class TopicTree(BERTopicVisualisation):
    """
    BERTopic text-based topic tree visualisation.

    Generates hierarchical topics from the supplied documents
    and renders the resulting BERTopic topic tree as text.
    """

    def __init__(
        self,
        output_dir=".",
        filename="topic_tree.txt",
        max_distance=None,
        tight_layout=False,
        text_column="Document",
        **params
    ):

        super().__init__(
            name="topic_tree",
            title=None,
            output_dir=output_dir,
            filename=filename,
            method="get_topic_tree",
            output_type="text",
            **params
        )

        self.max_distance = max_distance
        self.tight_layout = tight_layout
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
            # Extract document strings
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
                f"Generating topic hierarchy from "
                f"{len(docs):,} documents"
            )


            # --------------------------------------------------
            # Generate hierarchical topics
            # --------------------------------------------------

            hierarchical_topics = (
                bertopic_model.hierarchical_topics(
                    docs
                )
            )


            self.logger.info(
                "Generated hierarchical topics"
            )


            # --------------------------------------------------
            # Generate topic tree
            # --------------------------------------------------

            tree = bertopic_model.get_topic_tree(
                hierarchical_topics,
                max_distance=self.max_distance,
                tight_layout=self.tight_layout,
                **plot_kwargs
            )


        except Exception as e:

            self.logger.exception(
                f"Failed generating {self.name}: {e}"
            )

            return None


        return self._save(
            tree,
            save_path,
            filename
        )
