# visualisations/topic_barchart.py

from .bertopic_visualisation import BERTopicVisualisation


class TopicBarChart(BERTopicVisualisation):
    """
    BERTopic topic word barchart visualisation.
    """

    def __init__(
        self,
        output_dir=".",
        filename="topic_barchart.html",
        **params
    ):

        super().__init__(
            name="topic_barchart",
            title="",
            output_dir=output_dir,
            filename=filename,
            method="visualize_barchart",
            **params
        )


    def plot(
        self,
        data,
        metadata=None,
        model=None,
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