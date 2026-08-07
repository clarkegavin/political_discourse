import mlflow
import os

from logs.logger import get_logger


class MLflowReader:


    def __init__(
        self,
        tracking_uri=None
    ):

        self.logger = get_logger(
            self.__class__.__name__
        )

        if tracking_uri:
            mlflow.set_tracking_uri(
                tracking_uri
            )


    def load_runs(
        self,
        experiment_id=None,
        run_list=None
    ):

        client = mlflow.tracking.MlflowClient()


        # ---------------------------------
        # Specific runs requested
        # ---------------------------------

        if run_list:

            runs = []

            for run_id in run_list:

                run = client.get_run(
                    run_id
                )

                runs.append(
                    self._extract_run(run)
                )

            return runs


        # ---------------------------------
        # Specific experiment
        # ---------------------------------

        if experiment_id:
            runs = client.search_runs(
                experiment_ids=[
                    str(experiment_id)
                ]
            )

            return [
                self._extract_run(run)
                for run in runs
            ]


        # ---------------------------------
        # Everything
        # ---------------------------------

        experiments = client.search_runs(
            experiment_ids=[
                exp.experiment_id
                for exp in client.search_experiments()
            ]
        )

        return [
            self._extract_run(run)
            for run in experiments
        ]


    def _extract_run(
        self,
        run
    ):

        return {

            "run_id":
                run.info.run_id,

            "run_name":
                run.data.tags.get(
                    "mlflow.runName"
                ),

            "params":
                dict(run.data.params),

            "metrics":
                dict(run.data.metrics),

            "tags":
                dict(run.data.tags)

        }