import matplotlib.pyplot as plt
import pandas as pd

from visualisations.base import Visualisation


class MetricLineChart(Visualisation):

    def __init__(
        self,
        title="",
        figsize=(12, 8),
        rows=None,
        cols=None,
    ):

        super().__init__(
            title,
            figsize
        )

        self.figsize = figsize
        self.rows = rows
        self.cols = cols

    def plot(
        self,
        data,
        parameter,
        metrics,
        rows=2,
        cols=3,
        **kwargs
    ):

        # ---------------------------------
        # Validation
        # ---------------------------------

        if parameter not in data.columns:

            raise ValueError(
                f"Parameter '{parameter}' "
                f"not found in dataframe."
            )

        missing_metrics = [
            metric
            for metric in metrics
            if metric not in data.columns
        ]

        if missing_metrics:

            raise ValueError(
                f"Metrics not found in dataframe: "
                f"{missing_metrics}"
            )

        num_metrics = len(metrics)

        if rows * cols < num_metrics:

            raise ValueError(
                f"The configured subplot grid "
                f"({rows}x{cols}) cannot contain "
                f"{num_metrics} metrics."
            )

        # ---------------------------------
        # Prepare data
        # ---------------------------------

        data = data.copy()

        # ---------------------------------
        # Remove null / empty parameter values
        # ---------------------------------

        parameter_values = (
            data[parameter]
            .astype(str)
            .str.strip()
        )

        valid_parameter = (
            data[parameter].notna()
            &
            parameter_values.ne("")
            &
            parameter_values.str.lower().ne(
                "none"
            )
        )

        data = data[
            valid_parameter
        ].copy()

        if data.empty:

            raise ValueError(
                f"No populated values available "
                f"for parameter '{parameter}'."
            )

        # ---------------------------------
        # Convert parameter to numeric
        # ---------------------------------

        numeric_values = pd.to_numeric(
            data[parameter],
            errors="coerce"
        )

        if numeric_values.notna().all():

            data[parameter] = numeric_values

        else:

            raise ValueError(
                f"MetricLineChart received "
                f"non-numeric values for "
                f"parameter '{parameter}'."
            )

        # ---------------------------------
        # Sort parameter
        # ---------------------------------

        data = self._sort_parameter(
            data,
            parameter
        )

        # ---------------------------------
        # Create figure
        # ---------------------------------

        fig, axes = plt.subplots(
            rows,
            cols,
            figsize=self.figsize,
            squeeze=False
        )

        axes = axes.flatten()

        # ---------------------------------
        # Generate metric subplots
        # ---------------------------------

        for index, metric in enumerate(
            metrics
        ):

            ax = axes[index]

            # Convert metric to numeric.
            # This prevents strings such as
            # "None" from interfering with
            # aggregation.
            data[metric] = pd.to_numeric(
                data[metric],
                errors="coerce"
            )

            grouped = (
                data
                .groupby(
                    parameter,
                    dropna=True
                )[metric]
                .mean()
                .reset_index()
            )

            # Remove any invalid x-axis values
            # after grouping as a final safeguard.
            grouped = grouped[
                grouped[parameter].notna()
            ].copy()

            grouped = self._sort_parameter(
                grouped,
                parameter
            )

            if grouped.empty:

                self.logger.warning(
                    "No valid data available "
                    "for metric '%s' and "
                    "parameter '%s'.",
                    metric,
                    parameter
                )

                continue

            # ---------------------------------
            # Plot line
            # ---------------------------------

            ax.plot(
                grouped[parameter],
                grouped[metric],
                marker="o",
                linewidth=2
            )

            ax.set_title(
                metric
            )

            ax.set_xlabel(
                parameter
            )

            ax.set_ylabel(
                "Score"
            )

            ax.grid(
                axis="y",
                alpha=0.2
            )

        # ---------------------------------
        # Remove unused axes
        # ---------------------------------

        for index in range(
            num_metrics,
            len(axes)
        ):

            fig.delaxes(
                axes[index]
            )

        # ---------------------------------
        # Figure title
        # ---------------------------------

        if self.title:

            fig.suptitle(
                self.title
            )

        fig.tight_layout(
            rect=[
                0,
                0,
                1,
                0.96
            ]
        )

        return fig, axes

    def _sort_parameter(
        self,
        data,
        parameter
    ):

        data = data.copy()

        numeric_values = pd.to_numeric(
            data[parameter],
            errors="coerce"
        )

        # ---------------------------------
        # Numeric parameter
        # ---------------------------------

        if numeric_values.notna().all():

            data[parameter] = (
                numeric_values
            )

            return data.sort_values(
                parameter
            )

        # ---------------------------------
        # Categorical parameter
        # ---------------------------------

        return data.sort_values(
            parameter
        )