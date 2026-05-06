# tests/text_experiment_runner_dryrun.py
from experiments.runner import ExperimentRunner
from experiments.factory import ExperimentFactory


class DummyTextExperiment:
    def __init__(self, name: str = "dummy_text", X=None, text_field: str = "text", **kwargs):
        self.name = name
        self.X = X
        self.text_field = text_field
        self.kwargs = kwargs

    def run(self):
        # Simulate producing a DataFrame-like output without importing pandas
        # Return metadata including text field info so tests can assert correct propagation
        return {"df": None, "metadata": {"name": self.name, "text_field": self.text_field, "kwargs": self.kwargs}}


def test_text_dryrun():
    # Register dummy text experiment under a custom key
    ExperimentFactory.register_experiment("text_experiment", DummyTextExperiment)

    runner = ExperimentRunner(mlflow_enabled=False)

    experiments = [
        {"run_name": "t1", "params": {"text_field": "body"}},
        {"run_name": "t2", "params": {"text_field": "title"}}
    ]

    results = runner.run_experiments("text_experiment", experiments)
    assert len(results) == 2
    assert results[0]["run_name"] == "t1"
    assert "result" in results[0]
    assert results[0]["result"]["metadata"]["text_field"] == "body"
    assert results[1]["result"]["metadata"]["text_field"] == "title"

