# tests/test_experiment_pipeline_integration.py
import pandas as pd
from models.factory import ModelFactory
from experiments.factory import ExperimentFactory
from experiments.topic_modeling_experiment import TopicModelingExperiment
from pipelines.experiment_pipeline import ExperimentPipeline


class DummySupervisedExperiment:
    def __init__(self, name="dexp", X_train=None, X_test=None, y_train=None, y_test=None, **kwargs):
        self.name = name
        self.X_train = X_train
        self.X_test = X_test
        self.y_train = y_train
        self.y_test = y_test

    def run(self):
        return {"df": None, "metadata": {"name": self.name}}


def test_pipeline_integration_runs_experiment(monkeypatch):
    # Register a dummy experiment type
    ExperimentFactory.register_experiment("supervised", DummySupervisedExperiment)

    pipeline = ExperimentPipeline(
        experiment_type="supervised",
        model_name="mymodel",
        evaluator_name="dummyeval",
        metrics=["acc"],
        experiments=[{"run_name": "r1", "params": {}, "text_field": "text"}],
        mlflow_experiment=None,
        name="test_pipeline",
        global_config={}
    )

    X_train = pd.DataFrame({"text": ["t1", "t2"]})
    X_test = pd.DataFrame({"text": ["t3"]})
    y_train = [0, 1]
    y_test = [1]

    # Monkeypatch runner to capture run_experiments call and return success
    import pipelines.experiment_pipeline as pep

    def fake_run(self, experiment_type, exps, global_config=None, X=None):
        return [{"run_name": exps[0]['run_name'], "result": {"df": None, "metadata": {}}}]

    monkeypatch.setattr(pep.ExperimentRunner, 'run_experiments', fake_run)

    pipeline.execute(X_train, X_test, y_train, y_test)

    # If no exception, test passes
    assert True
