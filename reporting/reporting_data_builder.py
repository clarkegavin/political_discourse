import pandas as pd

from logs.logger import get_logger
from reporting.mlflow_reader import MLflowReader


class ReportingDataBuilder:

    def __init__(
            self,
            **kwargs
    ):

        self.logger = get_logger(
            self.__class__.__name__
        )

        self.config = kwargs

    def build(self):

        reader = MLflowReader(
            tracking_uri=self.config.get(
                "tracking_uri"
            )
        )

        runs = reader.load_runs(
            experiment_id=self.config.get(
                "experiment_id"
            ),
            experiment_ids=self.config.get(
                "experiment_ids"
            ),
            run_list=self.config.get(
                "run_list"
            ),
            filter_expression=self.config.get(
                "filter_expression"
            )
        )

        self.logger.info(
            f"Loaded {len(runs)} MLflow runs"
        )

        # ---------------------------------
        # Optional reporting selection
        # ---------------------------------

        runs = self._apply_selection(
            runs,
            self.config.get(
                "selection"
            )
        )

        self.logger.info(
            f"{len(runs)} MLflow runs remain "
            f"after reporting selection"
        )

        # ---------------------------------
        # Build reporting dataframe
        # ---------------------------------

        rows = []

        for run in runs:

            row = {}

            for field_config in self.config["fields"]:
                value = self._resolve_field(
                    run,
                    field_config["field"]
                )

                row[field_config["name"]] = value

            # ---------------------------------
            # Optional derived fields
            # ---------------------------------

            if self.config.get(
                    "derive_component",
                    False
            ):
                row["Component"] = (
                    self._resolve_component(
                        run
                    )
                )

            rows.append(row)

        dataframe = pd.DataFrame(
            rows
        )

        self.logger.info(
            f"Reporting dataframe contains "
            f"{len(dataframe)} rows and "
            f"{len(dataframe.columns)} columns"
        )

        return dataframe

    def _apply_selection(
            self,
            runs,
            selection
    ):

        if not selection:
            return runs

        group_by = selection.get(
            "group_by"
        )

        rank_by = selection.get(
            "rank_by"
        )

        limit = selection.get(
            "limit"
        )

        direction = selection.get(
            "direction",
            "desc"
        )

        if not group_by:
            raise ValueError(
                "Reporting selection requires "
                "'group_by'."
            )

        if not rank_by:
            raise ValueError(
                "Reporting selection requires "
                "'rank_by'."
            )

        if limit is None:
            raise ValueError(
                "Reporting selection requires "
                "'limit'."
            )

        if limit <= 0:
            raise ValueError(
                "Reporting selection 'limit' "
                "must be greater than zero."
            )

        if direction not in (
                "asc",
                "desc"
        ):
            raise ValueError(
                "Reporting selection 'direction' "
                "must be either 'asc' or 'desc'."
            )

        records = []

        for run in runs:
            records.append(
                {
                    "_run": run,

                    "_group":
                        self._resolve_field(
                            run,
                            group_by
                        ),

                    "_rank":
                        self._resolve_field(
                            run,
                            rank_by
                        )
                }
            )

        dataframe = pd.DataFrame(
            records
        )

        if dataframe.empty:
            return []

        # Metrics should normally be numeric,
        # but convert defensively.
        dataframe["_rank"] = pd.to_numeric(
            dataframe["_rank"],
            errors="coerce"
        )

        ascending = (
                direction == "asc"
        )

        dataframe = dataframe.sort_values(
            "_rank",
            ascending=ascending,
            na_position="last"
        )

        dataframe = (
            dataframe
            .groupby(
                "_group",
                sort=False,
                dropna=False
            )
            .head(limit)
        )

        selected_runs = (
            dataframe["_run"]
            .tolist()
        )

        self.logger.info(
            f"Selected top {limit} runs by "
            f"'{rank_by}' within "
            f"'{group_by}'"
        )

        return selected_runs


    def _resolve_field(
        self,
        run,
        field
    ):

        value = run

        for part in field.split("."):

            value = value.get(
                part,
                ""
            )

        return value

    def _create_experiment_label(self, run):

        params = run.get(
            "params",
            {}
        )

        model = params.get(
            "embedding_model_model_name",
            "unknown"
        )

        # shorten model names
        model = (
            model
            .split("/")[-1]
            .replace("-v1", "")
        )

        components = []

        for parameter in [
            "n_neighbors",
            "n_components",
            "min_dist",
            "distance_metric"
        ]:

            value = params.get(parameter)

            if value is not None:
                components.append(
                    f"{parameter}={value}"
                )

        if components:
            return (
                    f"{model}: "
                    + ", ".join(components)
            )

        return model

    def _resolve_component(
            self,
            run
    ):

        component_detection = self.config.get(
            "component_detection",
            {}
        )

        if not component_detection:
            self.logger.warning(
                "Component derivation is enabled but "
                "no 'component_detection' configuration "
                "was provided."
            )

            return "Baseline"

        matched_components = []

        for component, fields in (
                component_detection.items()
        ):

            for field in fields:

                value = self._resolve_field(
                    run,
                    field
                )

                if value not in (
                        None,
                        "",
                        "None"
                ):
                    matched_components.append(
                        component
                    )

                    break

        # ---------------------------------
        # Exactly one component matched
        # ---------------------------------

        if len(matched_components) == 1:
            return matched_components[0]

        # ---------------------------------
        # Multiple components matched
        # ---------------------------------

        if len(matched_components) > 1:
            self.logger.warning(
                "Run '%s' matched multiple "
                "components: %s",
                run.get("run_id"),
                matched_components
            )

            return "Multiple"

        # ---------------------------------
        # No component matched
        # ---------------------------------

        return "Baseline"