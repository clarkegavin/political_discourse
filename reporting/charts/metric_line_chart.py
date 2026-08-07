import os

from logs.logger import get_logger
from visualisations.factory import VisualisationFactory


class MetricLineChart:

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

        outputs = {}

        parameters = kwargs["parameters"]

        metrics = kwargs["metrics"].copy()

        #group_field = kwargs["group_field"]


        # -----------------------------
        # Invert metrics if required
        # -----------------------------

        data = data.copy()

        for metric in kwargs.get(
            "inverse_metrics",
            []
        ):

            if metric in data.columns:

                new_name = f"{metric} (inverted)"

                data[new_name] = (
                    1 - data[metric]
                )

                if metric in metrics:
                    metrics[
                        metrics.index(metric)
                    ] = new_name


        # -----------------------------
        # Generate one chart per parameter
        # -----------------------------

        for parameter in parameters:

            self.logger.info(
                f"Generating metric chart for {parameter}"
            )


            viz_params = kwargs.get(
                "visualisation",
                {}
            ).copy()


            viz_name = viz_params.pop(
                "name",
                "metric_line_chart"
            )


            visualisation = (
                VisualisationFactory
                .get_visualisation(
                    viz_name,
                    **viz_params
                )
            )

            fig, ax = visualisation.plot(
                data=data,
                parameter=parameter,
                metrics=metrics,
                rows=viz_params.get("rows", 2),
                cols=viz_params.get("cols", 3)
            )


            filename = (
                f"{kwargs.get('filename_prefix','metric')}"
                f"_{parameter}.png"
            )


            output_file = os.path.join(
                output_path,
                filename
            )


            visualisation.save(
                fig,
                output_file
            )


            outputs[parameter] = output_file


        return outputs