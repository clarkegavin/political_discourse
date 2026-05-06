# tests/test_experiment_runner_build_from_config_integration.py
from experiments.factory import ExperimentFactory
from experiments.runner import ExperimentRunner


class ConfigExp:
    def __init__(self, name=None, X=None, value=None, **kwargs):
        self.name = name
        self.X = X
        self.value = value

    def run(self):
        return {"df": self.X, "metadata": {"value": self.value}}


def test_runner_builds_experiment_from_config(monkeypatch):
    ExperimentFactory.register_experiment('config_exp', ConfigExp)

    runner = ExperimentRunner(mlflow_enabled=False)

    cfg = {"run_name": "cfg1", "params": {"value": 42, "X": 'DATA'}}

    results = runner.run_experiments('config_exp', [cfg])
    assert len(results) == 1
    assert 'result' in results[0]
    assert results[0]['result']['metadata']['value'] == 42

