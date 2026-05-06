# tests/test_experiment_runner_dryrun.py
from experiments.runner import ExperimentRunner
from experiments.factory import ExperimentFactory


class DummyExperiment:
    def __init__(self, name: str = "dummy", X=None, **kwargs):
        self.name = name
        self.X = X
        self.kwargs = kwargs

    def run(self):
        # return the DataFrame shape-like info without importing pandas to keep test lightweight
        return {"df": None, "metadata": {"name": self.name, "kwargs": self.kwargs}}


def test_dryrun_integration(monkeypatch):
    # Register dummy experiment
    ExperimentFactory.register_experiment("dummy", DummyExperiment)

    runner = ExperimentRunner(mlflow_enabled=False)
    experiments = [
        {"run_name": "d1", "params": {"some_param": 1}},
        {"run_name": "d2", "params": {"some_param": 2}}
    ]

    results = runner.run_experiments("dummy", experiments)
    assert len(results) == 2
    assert results[0]["run_name"] == "d1"
    assert "result" in results[0]
    assert results[0]["result"]["metadata"]["name"] == "d1"

