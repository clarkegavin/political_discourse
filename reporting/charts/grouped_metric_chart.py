import os

from visualisations.factory import VisualisationFactory
from logs.logger import get_logger


class GroupedMetricChart:

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
        # Apply inverse metrics
        # -----------------------------
        inverse_metrics = kwargs.get(
            "inverse_metrics",
            []
        )

        metrics = kwargs["metrics"].copy()

        for metric in inverse_metrics:

            if metric in data.columns:

                inverse_name = f"{metric} (inverted)"

                data[inverse_name] = 1 - data[metric]

                metrics[
                    metrics.index(metric)
                ] = inverse_name

            else:
                self.logger.warning(
                    f"Cannot invert metric '{metric}', column not found"
                )

        viz_params = kwargs.get(
            "visualisation",
            {}
        ).copy()


        viz_name = viz_params.pop(
            "name",
            "grouped_bar_chart"
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
            x_field=kwargs["x_field"],
            metrics=metrics,
            group_labels=kwargs.get(
                "group_labels"
            ),
            group_order=kwargs.get(
                "group_order"
            )
        )


        filename = kwargs.get(
            "filename",
            "metric_comparison.png"
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