import mlflow

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
        experiment_ids=None,
        run_list=None,
        filter_expression=None
    ):

        client = mlflow.tracking.MlflowClient()

        # ---------------------------------
        # Specific runs requested
        # ---------------------------------

        if run_list:

            if filter_expression:
                raise ValueError(
                    "Cannot use both 'run_list' and "
                    "'filter_expression'."
                )

            if experiment_id or experiment_ids:
                raise ValueError(
                    "Cannot use 'run_list' with "
                    "'experiment_id' or 'experiment_ids'."
                )

            runs = []

            for run_id in run_list:

                run = client.get_run(
                    run_id
                )

                runs.append(
                    self._extract_run(run)
                )

            self.logger.info(
                f"Loaded {len(runs)} specific MLflow runs"
            )

            return runs

        # ---------------------------------
        # Multiple experiments requested
        # ---------------------------------

        if experiment_ids:

            if experiment_id:
                raise ValueError(
                    "Cannot use both 'experiment_id' and "
                    "'experiment_ids'."
                )

            runs = client.search_runs(
                experiment_ids=[
                    str(exp_id)
                    for exp_id in experiment_ids
                ],
                filter_string=filter_expression
                if filter_expression
                else None
            )

            self.logger.info(
                f"Loaded {len(runs)} MLflow runs "
                f"from experiments {experiment_ids}"
            )

            if filter_expression:
                self.logger.info(
                    f"Applied MLflow filter: "
                    f"{filter_expression}"
                )

            return [
                self._extract_run(run)
                for run in runs
            ]

        # ---------------------------------
        # Single Experiment runs
        # ---------------------------------

        if experiment_id:

            runs = client.search_runs(
                experiment_ids=[
                    str(experiment_id)
                ],
                filter_string=filter_expression
                if filter_expression
                else None
            )

            self.logger.info(
                f"Loaded {len(runs)} MLflow runs "
                f"from experiment {experiment_id}"
            )

            if filter_expression:
                self.logger.info(
                    f"Applied MLflow filter: "
                    f"{filter_expression}"
                )

            return [
                self._extract_run(run)
                for run in runs
            ]

        # ---------------------------------
        # Everything
        # ---------------------------------

        experiment_ids = [
            exp.experiment_id
            for exp in client.search_experiments()
        ]

        runs = client.search_runs(
            experiment_ids=experiment_ids,
            filter_string=filter_expression
            if filter_expression
            else None
        )

        self.logger.info(
            f"Loaded {len(runs)} MLflow runs "
            f"across all experiments"
        )

        if filter_expression:
            self.logger.info(
                f"Applied MLflow filter: "
                f"{filter_expression}"
            )

        return [
            self._extract_run(run)
            for run in runs
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