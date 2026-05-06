# tests/test_experiment_runner_templating_nested.py
from experiments.runner import ExperimentRunner


def test_format_run_name_with_nested_params():
    runner = ExperimentRunner(mlflow_enabled=False)

    params = {
        "model": {"param": "X"},
        "nested": {"a": 1, "b": "two"},
        "simple": "s"
    }

    template = "run_{model_param}_{nested_a}_{simple}"
    formatted = runner._format_run_name(template, params)

    assert formatted == "run_X_1_s"

