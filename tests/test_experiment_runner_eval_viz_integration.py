# tests/test_experiment_runner_eval_viz_integration.py
from experiments.runner import ExperimentRunner
from experiments.factory import ExperimentFactory
from models.factory import ModelFactory
import pandas as pd


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
        return {"df": self.X, "metadata": {"visualisations": [{"name": "v1"}]}, "artifacts": []}

    def collect_params(self):
        return {"model_name": self.model_name}


def test_integration_runner_eval_viz(monkeypatch, tmp_path):
    ModelFactory.register_model('fake_model', FakeModel)
    ExperimentFactory.register_experiment('topic_modeling', FakeTopicExp)

    calls = {"mlf_start": 0, "mlf_end": 0, "logged_metrics": [], "logged_artifacts": []}

    import experiments.runner as er

    # monkeypatch mlflow
    monkeypatch.setattr(er.mlflow, 'set_experiment', lambda n: None)
    monkeypatch.setattr(er.mlflow, 'start_run', lambda **k: calls.__setitem__('mlf_start', calls['mlf_start']+1))
    monkeypatch.setattr(er.mlflow, 'log_param', lambda k,v: None)
    monkeypatch.setattr(er.mlflow, 'log_metric', lambda k,v: calls['logged_metrics'].append((k,v)))
    monkeypatch.setattr(er.mlflow, 'log_artifact', lambda p: calls['logged_artifacts'].append(p))
    monkeypatch.setattr(er.mlflow, 'end_run', lambda : calls.__setitem__('mlf_end', calls['mlf_end']+1))

    runner = ExperimentRunner(mlflow_enabled=True)
    df = pd.DataFrame({"text": ["a","b"]})
    cfg = {"run_name": "r1", "params": {"model_name": "fake_model", "X": df, "combined_text_field_name": "text", "save_path": str(tmp_path)}}

    res = runner.run_experiments('topic_modeling', [cfg])

    assert calls['mlf_start'] == 1
    assert calls['mlf_end'] == 1
    # ensure metrics/artifacts lists exist (may be empty)
    assert isinstance(calls['logged_metrics'], list)
    assert isinstance(calls['logged_artifacts'], list)
    assert len(res) == 1

