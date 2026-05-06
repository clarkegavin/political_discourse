# tests/test_experiment_runner_retries.py
from experiments.runner import ExperimentRunner
from experiments.factory import ExperimentFactory


class FlakyExp:
    def __init__(self, name='flaky', **kwargs):
        self.calls = 0

    def run(self):
        self.calls += 1
        if self.calls < 2:
            raise RuntimeError("transient")
        return {"df": None, "metadata": {}, "artifacts": []}


def test_runner_retries(monkeypatch):
    ExperimentFactory.register_experiment('flaky', FlakyExp)

    runner = ExperimentRunner(mlflow_enabled=False)
    cfg = {"run_name": "r1", "params": {}}

    results = runner.run_experiments('flaky', [{"run_name": "r1", "params": {}, "retries": 1}])
    assert len(results) == 1
    assert 'result' in results[0]

