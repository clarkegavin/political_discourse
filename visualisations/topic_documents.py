from .bertopic_visualisation import BERTopicVisualisation


class TopicDocuments(BERTopicVisualisation):
    """
    BERTopic document projection visualisation.
    """

    def __init__(
        self,
        output_dir=".",
        filename="topic_documents.html",
        **params
    ):

        super().__init__(
            name="topic_documents",
            title="Topic Documents",
            output_dir=output_dir,
            filename=filename,
            method="visualize_documents",
            **params
        )


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

        representation_field = metadata.get(
            "representation_text_field"
        )

        if representation_field:
            docs = data[representation_field].tolist()
        else:
            raise ValueError(
                "No representation_text_field supplied"
            )

        fig = bertopic_model.visualize_documents(
            docs,
            **self.params,
            **plot_kwargs
        )


        return self._save(
            fig,
            save_path,
            filename
        )