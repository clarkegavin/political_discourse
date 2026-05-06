# tests/test_experiment_pipeline_unit.py
import pandas as pd
from pipelines.experiment_pipeline import ExperimentPipeline


def test_experiment_pipeline_delegates_to_runner(monkeypatch):
    # Prepare a minimal pipeline config
    pipeline = ExperimentPipeline(
        experiment_type="dummy",
        model_name="mymodel",
        evaluator_name="dummy_eval",
        metrics=["m"],
        experiments=[{"run_name": "r1", "params": {"foo": "bar"}, "text_field": "text"}],
        mlflow_experiment=None,
        name="test_pipeline",
        global_config={}
    )

    # Small train/test data
    X_train = pd.DataFrame({"text": ["a", "b"]})
    X_test = pd.DataFrame({"text": ["c"]})
    y_train = [0, 1]
    y_test = [1]

    called = {}

    def fake_run_experiments(self, experiment_type, exps, global_config=None, X=None):
        called['called'] = True
        # Return a dummy result
        return [{"run_name": exps[0]['run_name'], "result": {"df": None, "metadata": {}}}]

    monkeypatch.setattr('pipelines.experiment_pipeline.ExperimentRunner.run_experiments', fake_run_experiments)

    pipeline.execute(X_train, X_test, y_train, y_test)

    assert called.get('called', False) is True
