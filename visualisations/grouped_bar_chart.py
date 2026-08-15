import matplotlib.pyplot as plt

from visualisations.base import Visualisation


class GroupedBarChart(Visualisation):

    def __init__(
        self,
        title="",
        figsize=(10, 6),
        **kwargs
    ):
        super().__init__(
            title,
            figsize
        )

        self.y_label = kwargs.get(
            "y_label",
            "Score"
        )


    def plot(
        self,
        data,
        x_field,
        metrics,
        group_order=None,
        **kwargs
    ):

        data = data.copy()

        # ---------------------------------
        # Validate x field
        # ---------------------------------

        if x_field not in data.columns:

            raise ValueError(
                f"X field '{x_field}' "
                f"not found in dataframe."
            )

        # ---------------------------------
        # Apply explicit group ordering
        # ---------------------------------

        if group_order is not None:

            data_groups = set(
                data[x_field]
                .dropna()
            )

            ordered_groups = set(
                group_order
            )

            unknown_order_values = (
                ordered_groups
                - data_groups
            )

            missing_order_values = (
                data_groups
                - ordered_groups
            )

            if unknown_order_values:

                raise ValueError(
                    f"Group order for field "
                    f"'{x_field}' contains values "
                    f"not present in the data: "
                    f"{unknown_order_values}"
                )

            if missing_order_values:

                raise ValueError(
                    f"Group order for field "
                    f"'{x_field}' is missing values "
                    f"present in the data: "
                    f"{missing_order_values}"
                )

            order_map = {
                value: index
                for index, value
                in enumerate(group_order)
            }

            data["_group_order"] = (
                data[x_field]
                .map(order_map)
            )

            data = data.sort_values(
                "_group_order"
            ).drop(
                columns="_group_order"
            )

        # ---------------------------------
        # Create figure
        # ---------------------------------

        fig, ax = plt.subplots(
            figsize=self.figsize
        )

        x = range(
            len(data[x_field])
        )

        group_labels = kwargs.get(
            "group_labels"
        )

        rotation = kwargs.get(
            "rotation"
        )

        num_metrics = len(
            metrics
        )

        width = (
            0.8 / num_metrics
        )

        # ---------------------------------
        # Plot metrics
        # ---------------------------------

        for index, metric in enumerate(
            metrics
        ):

            positions = [
                i + index * width
                for i in x
            ]

            ax.bar(
                positions,
                data[metric],
                width,
                label=metric
            )

        # ---------------------------------
        # X-axis
        # ---------------------------------

        ax.set_xticks(
            [
                i + width * (
                    num_metrics - 1
                ) / 2
                for i in x
            ]
        )

        if group_labels is None:

            labels = data[x_field]

        else:

            labels = [
                group_labels.get(
                    value,
                    value
                )
                for value in data[x_field]
            ]

        ax.set_xticklabels(
            labels,
            rotation=rotation,
            ha="right"
        )

        # ---------------------------------
        # Formatting
        # ---------------------------------

        ax.set_ylabel(
            self.y_label
        )

        ax.set_title(
            self.title
        )

        ax.legend(
            loc="lower center",
            bbox_to_anchor=(0.5, 1.02),
            ncol=len(metrics)
        )

        fig.tight_layout(
            rect=[
                0,
                0,
                1,
                0.9
            ]
        )

        return fig, ax