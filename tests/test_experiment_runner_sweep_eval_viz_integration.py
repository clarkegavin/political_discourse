# tests/test_experiment_runner_sweep_eval_viz_integration.py
import pandas as pd
from experiments.runner import ExperimentRunner
from experiments.factory import ExperimentFactory
from models.factory import ModelFactory


class FakeModel:
    def __init__(self, name, **kwargs):
        pass
    def fit_transform(self, X):
        return [0 for _ in range(len(X))]
    def get_topic_info(self):
        import pandas as pd
        return pd.DataFrame({"Topic": [0], "Name": ["t0"], "Count": [1]})
    def get_topics(self):
        return {0: [("a", 1.0)]}


class FakeTopicExp:
    def __init__(self, name='fte', X=None, model_name=None, combined_text_field_name='text', save_path=None, **kwargs):
        self.name = name
        self.X = X
        self.model_name = model_name
        self.combined_text_field_name = combined_text_field_name
        self.save_path = save_path

    def run(self):
        return {"df": self.X, "metadata": {"visualisations": [{"name": "v1", "init": {}, "plot": {}}], "evaluator_name": "ev1"}, "artifacts": []}

    def collect_params(self):
        return {"model_name": self.model_name}


# Simple fake evaluator and viz to ensure runners are invoked
from evaluators.factory import EvaluatorFactory
from visualisations.factory import VisualisationFactory


class FakeEvaluator:
    def __init__(self, name=None, **kwargs):
        pass
    def evaluate(self, df, metadata, params=None):
        return {"score": 0.9}


class FakeViz:
    def __init__(self, **kwargs):
        pass
    def plot(self, df, model=None, **plot_kwargs):
        # return a fake artifact path
        return ["/tmp/fake_viz.png"]


def test_sweep_with_eval_and_viz(monkeypatch, tmp_path):
    # register model, experiment, evaluator, viz
    ModelFactory.register_model('fake_model', FakeModel)
    ExperimentFactory.register_experiment('topic_modeling', FakeTopicExp)
    EvaluatorFactory.register_evaluator('ev1', FakeEvaluator)
    VisualisationFactory.register_visualisation('v1', FakeViz)

    # monkeypatch mlflow to capture logs
    calls = {"metrics": [], "artifacts": []}
    import experiments.runner as er
    monkeypatch.setattr(er.mlflow, 'set_experiment', lambda n: None)
    monkeypatch.setattr(er.mlflow, 'start_run', lambda **k: None)
    monkeypatch.setattr(er.mlflow, 'log_param', lambda k,v: None)
    monkeypatch.setattr(er.mlflow, 'log_metric', lambda k,v: calls['metrics'].append((k,v)))
    monkeypatch.setattr(er.mlflow, 'log_artifact', lambda p: calls['artifacts'].append(p))
    monkeypatch.setattr(er.mlflow, 'end_run', lambda : None)

    runner = ExperimentRunner(mlflow_enabled=True)

    df = pd.DataFrame({"text": ["a","b"]})
    experiments = [
        {
            "run_name": "sweep_{i}",
            "sweep": {"params.i": [1,2]},
            "overrides": [{"params.extra": 10}, {"params.extra": 20}],
            "params": {"i": 0, "params": {"model_name": "fake_model"}, "X": df, "combined_text_field_name": "text", "save_path": str(tmp_path)}
        }
    ]

    results = runner.run_experiments('topic_modeling', experiments)
    assert len(results) == 4  # 2 sweep values * 2 overrides
    assert len(calls['metrics']) >= 0
    assert all(isinstance(r, dict) for r in results)
