from .bertopic_visualisation import BERTopicVisualisation


class TopicHeatmap(BERTopicVisualisation):
    """
    BERTopic topic similarity heatmap visualisation.
    """

    def __init__(
        self,
        output_dir=".",
        filename="topic_heatmap.html",
        **params
    ):

        super().__init__(
            name="topic_heatmap",
            title="Topic Heatmap",
            output_dir=output_dir,
            filename=filename,
            method="visualize_heatmap",
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