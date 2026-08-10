import os

from visualisations.factory import VisualisationFactory
from logs.logger import get_logger


class MetricHeatmap:

    def __init__(self):

        self.logger = get_logger(
            self.__class__.__name__
        )


    def run(
        self,
        data,
        output_path,
        **kwargs
    ):

        data = data.copy()


        # -----------------------------
        # Optional transformations
        # -----------------------------

        inverse_metrics = kwargs.get(
            "inverse_metrics",
            []
        )


        metrics = kwargs["metrics"].copy()


        for metric in inverse_metrics:

            if metric in data.columns:

                new_name = f"{metric} (inverted)"

                data[new_name] = (
                    1 - data[metric]
                )


                metrics[
                    metrics.index(metric)
                ] = new_name



        # -----------------------------
        # Create visualisation
        # -----------------------------

        viz_params = kwargs.get(
            "visualisation",
            {}
        ).copy()


        viz_name = viz_params.pop(
            "name",
            "metric_heatmap"
        )


        visualisation = VisualisationFactory.get_visualisation(
            viz_name,
            **viz_params
        )


        if visualisation is None:
            raise KeyError(
                f"Visualisation '{viz_name}' not registered"
            )


        fig, ax = visualisation.plot(
            data=data,
            index_field=kwargs["index_field"],
            metrics=metrics,
            group_labels=kwargs.get(
                "group_labels"
            )
        )


        filename = kwargs.get(
            "filename",
            "metric_heatmap.png"
        )


        output_file = os.path.join(
            output_path,
            filename
        )


        visualisation.save(
            fig,
            output_file
        )


        return output_file