from .bertopic_visualisation import BERTopicVisualisation


class TopicHierarchy(BERTopicVisualisation):
    """
    BERTopic hierarchical topic visualisation.
    """

    def __init__(
        self,
        output_dir=".",
        filename="topic_hierarchy.html",
        **params
    ):

        super().__init__(
            name="topic_hierarchy",
            title="Topic Hierarchy",
            output_dir=output_dir,
            filename=filename,
            method="visualize_hierarchy",
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

        return self._render(
            model=model,
            save_path=save_path,
            filename=filename,
            **plot_kwargs
        )