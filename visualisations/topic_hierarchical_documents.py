from .bertopic_visualisation import BERTopicVisualisation


class HierarchicalDocuments(BERTopicVisualisation):
    """
    BERTopic hierarchical document visualisation.
    """

    def __init__(
        self,
        output_dir=".",
        filename="hierarchical_documents.html",
        text_column="RepresentationDocument",
        **params
    ):

        super().__init__(
            name="hierarchical_documents",
            title="",
            output_dir=output_dir,
            filename=filename,
            method="visualize_hierarchical_documents",
            **params
        )

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
                f"Generating hierarchical topics from "
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


            # --------------------------------------------------
            # Build visualisation parameters
            # --------------------------------------------------

            visualisation_params = {
                "docs": docs,
                "hierarchical_topics": hierarchical_topics,
                "title": self.title,
                **self.params,
                **plot_kwargs
            }


            # --------------------------------------------------
            # Generate visualisation
            # --------------------------------------------------

            fig = (
                bertopic_model.visualize_hierarchical_documents(
                    **visualisation_params
                )
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