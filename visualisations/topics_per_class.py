from .bertopic_visualisation import BERTopicVisualisation


class TopicsPerClass(BERTopicVisualisation):
    """
    BERTopic topics per class visualisation.

    Creates a topics-per-class dataframe using BERTopic's
    topics_per_class() method, then renders it using
    visualize_topics_per_class().

    Parameters:
        classes_column:
            DataFrame column containing class labels
            (e.g. party, forum, constituency)

        text_column:
            DataFrame column containing the documents used
            for BERTopic analysis.

        top_n_topics:
            Optional limit on number of topics displayed.
    """

    def __init__(
        self,
        output_dir=".",
        filename="topics_per_class.html",
        classes_column=None,
        text_column=None,
        top_n_topics=None,
        **params
    ):

        super().__init__(
            name="topics_per_class",
            title="",
            output_dir=output_dir,
            filename=filename,
            method="visualize_topics_per_class",
            **params
        )

        self.classes_column = classes_column
        self.text_column = text_column
        self.top_n_topics = top_n_topics

    def plot(
            self,
            data,
            model=None,
            metadata=None,
            save_path=None,
            filename=None,
            **plot_kwargs
    ):

        bertopic_model = self._get_bertopic_model(model)

        if bertopic_model is None:
            return None

        if self.text_column is None:
            self.logger.warning(
                "No text_column supplied for topics_per_class visualisation"
            )
            return None

        if self.classes_column is None:
            self.logger.warning(
                "No classes_column supplied for topics_per_class visualisation"
            )
            return None

        missing_columns = [
            col
            for col in [
                self.text_column,
                self.classes_column
            ]
            if col not in data.columns
        ]

        if missing_columns:
            self.logger.warning(
                f"Missing required columns for topics_per_class: {missing_columns}"
            )
            return None

        docs = (
            data[self.text_column]
            .fillna("")
            .tolist()
        )

        classes = (
            data[self.classes_column]
            .fillna("Unknown")
            .tolist()
        )

        try:

            topics_per_class = bertopic_model.topics_per_class(
                docs,
                classes=classes
            )

            if self.top_n_topics is not None:
                plot_kwargs["top_n_topics"] = self.top_n_topics

            fig = bertopic_model.visualize_topics_per_class(
                topics_per_class,
                title=self.title,
                **plot_kwargs
            )


        except Exception as e:

            self.logger.exception(
                f"Failed generating topics per class visualisation: {e}"
            )

            return None

        return self._save(
            fig,
            save_path,
            filename
        )