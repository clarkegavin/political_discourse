import os

from logs.logger import get_logger
from visualisations.factory import VisualisationFactory


class ComponentComparisonChart:

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

        embedding_field = kwargs.get(
            "embedding_field",
            "Embedding Model"
        )

        component_field = kwargs.get(
            "component_field",
            "Component"
        )

        rank_by = kwargs.get(
            "rank_by",
            "Coherence"
        )

        descending = kwargs.get(
            "descending",
            True
        )

        value_field = kwargs.get(
            "value_field",
            rank_by
        )

        # ---------------------------------
        # Validate fields
        # ---------------------------------

        required_fields = [
            embedding_field,
            component_field,
            rank_by,
            value_field
        ]

        missing_fields = [
            field
            for field in required_fields
            if field not in data.columns
        ]

        if missing_fields:

            raise ValueError(
                "Required fields missing from "
                f"reporting dataframe: "
                f"{missing_fields}"
            )

        # ---------------------------------
        # Select best run for each
        # embedding model / component
        # ---------------------------------

        best_data = self._select_best_runs(
            data=data,
            embedding_field=embedding_field,
            component_field=component_field,
            rank_by=rank_by,
            descending=descending
        )

        # ---------------------------------
        # Prepare chart data
        # ---------------------------------

        chart_data = (
            best_data
            .pivot(
                index=embedding_field,
                columns=component_field,
                values=value_field
            )
            .reset_index()
        )

        chart_data.columns.name = None

        components = [
            column
            for column in chart_data.columns
            if column != embedding_field
        ]

        if not components:

            raise ValueError(
                "No components found "
                "for component comparison chart."
            )

        self.logger.info(
            f"Component comparison groups: "
            f"{components}"
        )

        self.logger.info(
            f"Component comparison data:\n"
            f"{chart_data}"
        )

        # ---------------------------------
        # Build visualisation
        # ---------------------------------

        viz_params = kwargs.get(
            "visualisation",
            {}
        ).copy()

        viz_name = viz_params.pop(
            "name",
            "grouped_bar_chart"
        )

        visualisation = (
            VisualisationFactory
            .get_visualisation(
                viz_name,
                **viz_params
            )
        )

        if visualisation is None:

            raise ValueError(
                f"Visualisation '{viz_name}' "
                f"was not found in registry."
            )

        fig, ax = visualisation.plot(
            data=chart_data,
            x_field=embedding_field,
            metrics=components,
            group_labels=kwargs.get(
                "group_labels"
            )
        )

        # ---------------------------------
        # Save
        # ---------------------------------

        filename = kwargs.get(
            "filename",
            "component_comparison.png"
        )

        output_file = os.path.join(
            output_path,
            filename
        )

        visualisation.save(
            fig,
            output_file
        )

        self.logger.info(
            f"Component comparison chart "
            f"generated: {output_file}"
        )

        return output_file

    def _select_best_runs(
        self,
        data,
        embedding_field,
        component_field,
        rank_by,
        descending
    ):

        data = data.copy()

        data = data.dropna(
            subset=[
                embedding_field,
                component_field,
                rank_by
            ]
        )

        if data.empty:

            raise ValueError(
                "No valid data available for "
                "component comparison."
            )

        # ---------------------------------
        # Sort so first row in each group
        # is the best-performing experiment
        # ---------------------------------

        data = data.sort_values(
            by=[
                embedding_field,
                component_field,
                rank_by
            ],
            ascending=[
                True,
                True,
                not descending
            ]
        )

        # ---------------------------------
        # Select best experiment for each
        # embedding model / component
        # ---------------------------------

        best_data = (
            data
            .groupby(
                [
                    embedding_field,
                    component_field
                ],
                as_index=False
            )
            .first()
        )

        return best_data

    def _derive_component(self, run):

        if not self.config.get(
                "derive_component",
                False
        ):
            return None

        detection = self.config.get(
            "component_detection",
            {}
        )

        for component, fields in detection.items():

            for field in fields:

                value = self._resolve_field(
                    run,
                    field
                )

                if value not in (
                        None,
                        ""
                ):
                    return component

        return None