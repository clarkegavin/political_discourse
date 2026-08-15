from .bertopic_visualisation import BERTopicVisualisation


class TopicTermRank(BERTopicVisualisation):
    """
    BERTopic term rank visualisation.
    """

    def __init__(
        self,
        output_dir=".",
        filename="topic_term_rank.html",
        **params
    ):

        super().__init__(
            name="topic_term_rank",
            title="",
            output_dir=output_dir,
            filename=filename,
            method="visualize_term_rank",
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