# tests/test_experiment_runner_mlflow_integration.py
from experiments.runner import ExperimentRunner
from experiments.factory import ExperimentFactory


class DummyMLExperiment:
    def __init__(self, name: str = "dummy_ml", X=None, **kwargs):
        self.name = name
        self.X = X
        self.kwargs = kwargs

    def run(self):
        return {"df": None, "metadata": {"name": self.name, "kwargs": self.kwargs}}


def test_experiment_runner_manages_mlflow(monkeypatch):
    # Register dummy experiment
    ExperimentFactory.register_experiment("dummy_ml", DummyMLExperiment)

    experiments = [
        {"run_name": "r1", "params": {}, "mlflow_experiment": "expA"},
        {"run_name": "r2", "params": {}, "mlflow_experiment": "expB"}
    ]

    calls = {"set_experiment": [], "start_run": [], "log_param": [], "end_run": []}

    # Monkeypatch mlflow functions in the experiments.runner module
    import experiments.runner as er

    def fake_set_experiment(name):
        calls["set_experiment"].append(name)

    def fake_start_run(**kwargs):
        calls["start_run"].append(kwargs)

    def fake_log_param(k, v):
        calls["log_param"].append((k, v))

    def fake_end_run():
        calls["end_run"].append(True)

    monkeypatch.setattr(er.mlflow, "set_experiment", fake_set_experiment)
    monkeypatch.setattr(er.mlflow, "start_run", fake_start_run)
    monkeypatch.setattr(er.mlflow, "log_param", fake_log_param)
    monkeypatch.setattr(er.mlflow, "end_run", fake_end_run)

    runner = ExperimentRunner(mlflow_enabled=True)
    results = runner.run_experiments("dummy_ml", experiments)

    # Two runs -> start_run and end_run called twice
    assert len(calls["start_run"]) == 2
    assert len(calls["end_run"]) == 2

    # set_experiment called with each experiment name
    assert calls["set_experiment"] == ["expA", "expB"]

    # Results structure preserved
    assert len(results) == 2
    assert results[0]["run_name"] == "r1"
    assert "result" in results[0]

