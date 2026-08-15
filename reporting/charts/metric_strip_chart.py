import os

from logs.logger import get_logger
from visualisations.factory import VisualisationFactory


class MetricStripChart:

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

        group_field = kwargs[
            "group_field"
        ]

        metrics = kwargs[
            "metrics"
        ].copy()

        layout = kwargs.get(
            "layout",
            "combined"
        )

        if layout not in (
                "combined",
                "separate"
        ):
            raise ValueError(
                "MetricStripChart 'layout' must be "
                "'combined' or 'separate'."
            )

        # ---------------------------------
        # Validate group field
        # ---------------------------------

        if group_field not in data.columns:
            raise ValueError(
                f"Group field '{group_field}' "
                f"not found in reporting dataframe."
            )

        # ---------------------------------
        # Invert metrics
        # ---------------------------------

        data = data.copy()

        for metric in kwargs.get(
                "inverse_metrics",
                []
        ):

            if metric in data.columns:

                new_name = (
                    f"{metric} (inverted)"
                )

                data[new_name] = (
                        1 - data[metric]
                )

                if metric in metrics:
                    metrics[
                        metrics.index(metric)
                    ] = new_name

        # ---------------------------------
        # Generate combined figure
        # ---------------------------------

        if layout == "combined":
            viz_params = kwargs.get(
                "visualisation",
                {}
            ).copy()

            viz_name = viz_params.pop(
                "name",
                "metric_strip_plot"
            )

            visualisation = (
                VisualisationFactory
                .get_visualisation(
                    viz_name,
                    **viz_params
                )
            )

            fig, axes = visualisation.plot(
                data=data,
                group_field=group_field,
                metrics=metrics,
                rows=kwargs.get(
                    "rows",
                    2
                ),
                cols=kwargs.get(
                    "cols",
                    3
                ),
                central_tendency=kwargs.get(
                    "central_tendency"
                ),
                jitter=kwargs.get(
                    "jitter",
                    0.08
                ),
                group_labels=kwargs.get(
                    "group_labels"
                ),
                group_order=kwargs.get(
                    "group_order"
                )
            )

            filename = kwargs.get(
                "filename",
                "metric_strip_plot.png"
            )

            output_file = os.path.join(
                output_path,
                filename
            )

            visualisation.save(
                fig,
                output_file
            )

            outputs["combined"] = output_file

            return outputs

        # ---------------------------------
        # Generate separate figures
        # ---------------------------------

        filename_prefix = kwargs.get(
            "filename_prefix",
            "metric_strip_plot"
        )

        for metric in metrics:
            self.logger.info(
                f"Generating strip plot for "
                f"{metric}"
            )

            viz_params = kwargs.get(
                "visualisation",
                {}
            ).copy()

            viz_name = viz_params.pop(
                "name",
                "metric_strip_plot"
            )

            visualisation = (
                VisualisationFactory
                .get_visualisation(
                    viz_name,
                    **viz_params
                )
            )

            fig, axes = visualisation.plot(
                data=data,
                group_field=group_field,
                metrics=[metric],
                rows=1,
                cols=1,
                central_tendency=kwargs.get(
                    "central_tendency"
                ),
                jitter=kwargs.get(
                    "jitter",
                    0.08
                )
            )

            filename = (
                f"{filename_prefix}_"
                f"{metric}.png"
            )

            output_file = os.path.join(
                output_path,
                filename
            )

            visualisation.save(
                fig,
                output_file
            )

            outputs[metric] = output_file

        return outputs