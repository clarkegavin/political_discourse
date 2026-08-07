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
            run_list=self.config.get(
                "run_list"
            )
        )

        rows = []

        self.logger.info("Temporary logging...")
        for run in runs:
            self.logger.info(
                run["metrics"]
            )


        for run in runs:

            row = {}

            for field_config in self.config["fields"]:
                value = self._resolve_field(
                    run,
                    field_config["field"]
                )

                row[field_config["name"]] = value

            # Add readable experiment label
            # row["Experiment"] = (
            #     self._create_experiment_label(run)
            # )

            rows.append(row)

        return pd.DataFrame(rows)



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