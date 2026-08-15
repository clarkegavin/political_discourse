# visualisations/intertopic_distance.py
from .bertopic_visualisation import BERTopicVisualisation


class IntertopicDistance(BERTopicVisualisation):
    """
    BERTopic intertopic distance map visualisation.
    """

    def __init__(
        self,
        output_dir=".",
        filename="intertopic_distance.html",
        **params
    ):

        super().__init__(
            name="intertopic_distance",
            title="",
            output_dir=output_dir,
            filename=filename,
            method="visualize_topics",
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