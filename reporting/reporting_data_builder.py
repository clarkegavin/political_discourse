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

        for run in runs:

            row = {}

            for field_config in self.config["fields"]:
                value = self._resolve_field(
                    run,
                    field_config["field"]
                )

                row[field_config["name"]] = value

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