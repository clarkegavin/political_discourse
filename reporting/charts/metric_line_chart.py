import os
import pandas as pd

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

        parameters = kwargs[
            "parameters"
        ]

        metrics = kwargs[
            "metrics"
        ].copy()

        group_field = kwargs.get(
            "group_field"
        )

        # ---------------------------------
        # Validate group field
        # ---------------------------------

        if group_field:

            if group_field not in data.columns:

                raise ValueError(
                    f"Group field '{group_field}' "
                    f"not found in dataframe."
                )

            groups = (
                data[group_field]
                .dropna()
                .unique()
                .tolist()
            )

        else:

            # Preserve existing behaviour
            groups = [None]

        # ---------------------------------
        # Prepare data
        # ---------------------------------

        data = data.copy()

        # ---------------------------------
        # Invert metrics if required
        # ---------------------------------

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
        # Iterate over embedding models
        # ---------------------------------

        for group in groups:

            # ---------------------------------
            # Filter data for group
            # ---------------------------------

            if group is None:

                group_data = data.copy()

            else:

                group_data = data[
                    data[group_field] == group
                ].copy()

            if group_data.empty:

                self.logger.warning(
                    "No data found for group '%s'. "
                    "Skipping.",
                    group
                )

                continue

            # ---------------------------------
            # Iterate over parameters
            # ---------------------------------

            for parameter in parameters:

                self.logger.info(
                    "Generating metric chart "
                    "for group='%s', parameter='%s'",
                    group,
                    parameter
                )

                if parameter not in group_data.columns:

                    self.logger.warning(
                        "Parameter '%s' not found "
                        "for group '%s'. Skipping.",
                        parameter,
                        group
                    )

                    continue

                # ---------------------------------
                # Determine visualisation type
                # ---------------------------------

                numeric_values = pd.to_numeric(
                    group_data[parameter],
                    errors="coerce"
                )

                non_null_values = (
                    group_data[parameter]
                    .notna()
                )

                is_numeric = (
                    numeric_values[non_null_values]
                    .notna()
                    .all()
                )

                if is_numeric:

                    group_data[parameter] = numeric_values

                    viz_name = "metric_line_chart"

                else:

                    viz_name = "metric_bar_chart"

                # ---------------------------------
                # Visualisation configuration
                # ---------------------------------

                viz_params = kwargs.get(
                    "visualisation",
                    {}
                ).copy()

                # The configured visualisation name
                # is the default for numeric parameters.
                #
                # Categorical parameters are automatically
                # routed to metric_bar_chart.

                if viz_name == "metric_line_chart":

                    configured_name = viz_params.pop(
                        "name",
                        "metric_line_chart"
                    )

                    viz_name = configured_name

                else:

                    viz_params.pop(
                        "name",
                        None
                    )

                    viz_name = "metric_bar_chart"

                visualisation = (
                    VisualisationFactory
                    .get_visualisation(
                        viz_name,
                        **viz_params
                    )
                )

                if visualisation is None:

                    raise ValueError(
                        f"Visualisation "
                        f"'{viz_name}' not found "
                        "in registry."
                    )

                # ---------------------------------
                # Generate figure
                # ---------------------------------

                fig, axes = visualisation.plot(
                    data=group_data,
                    parameter=parameter,
                    metrics=metrics,
                    rows=kwargs.get(
                        "rows",
                        2
                    ),
                    cols=kwargs.get(
                        "cols",
                        3
                    )
                )

                # ---------------------------------
                # Build filename
                # ---------------------------------

                filename_prefix = kwargs.get(
                    "filename_prefix",
                    "metric"
                )

                filename_parts = [
                    filename_prefix
                ]

                if group is not None:

                    filename_parts.append(
                        self._safe_filename(
                            group
                        )
                    )

                filename_parts.append(
                    self._safe_filename(
                        parameter
                    )
                )

                filename = (
                    "_".join(
                        filename_parts
                    )
                    + ".png"
                )

                output_file = os.path.join(
                    output_path,
                    filename
                )

                # ---------------------------------
                # Save
                # ---------------------------------

                visualisation.save(
                    fig,
                    output_file
                )

                key_parts = []

                if group is not None:

                    key_parts.append(
                        str(group)
                    )

                key_parts.append(
                    str(parameter)
                )

                output_key = "_".join(
                    key_parts
                )

                outputs[
                    output_key
                ] = output_file

        return outputs

    @staticmethod
    def _safe_filename(
        value
    ):

        value = str(
            value
        )

        return (
            value
            .replace(
                " ",
                "_"
            )
            .replace(
                "/",
                "_"
            )
            .replace(
                "\\",
                "_"
            )
        )