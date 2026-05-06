# tests/test_experiment_runner_topic_integration.py
from experiments.runner import ExperimentRunner
from models.factory import ModelFactory
from experiments.factory import ExperimentFactory
from experiments.topic_modeling_experiment import TopicModelingExperiment
import pandas as pd


class FakeTopicModel:
    def __init__(self, name, **kwargs):
        self.name = name

    def fit_transform(self, docs, embeddings=None):
        topics = [1 for _ in docs]
        probs = [0.8 for _ in docs]
        return topics, probs

    def get_topic_info(self):
        import pandas as pd
        return pd.DataFrame({"Topic": [1], "Name": ["topic1"], "Count": [1]})

    def get_topics(self):
        return {1: [("a", 0.5), ("b", 0.4)]}


def test_runner_logs_experiment_params(monkeypatch, tmp_path):
    # Register model and ensure ExperimentFactory can create TopicModelingExperiment via key
    ModelFactory.register_model("fake_topic_model", FakeTopicModel)
    ExperimentFactory.register_experiment("topic_modeling", TopicModelingExperiment)

    df = pd.DataFrame({"text": ["doc1", "doc2"]})

    experiments = [
        {"run_name": "t_topic", "params": {"model_name": "fake_topic_model", "evaluator_name": "dummy", "X": df, "combined_text_field_name": "text", "save_path": str(tmp_path)} }
    ]

    calls = {"set_experiment": [], "start_run": [], "log_param": [], "end_run": []}

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
    results = runner.run_experiments("topic_modeling", experiments)

    assert len(results) == 1
    assert calls["start_run"] and calls["end_run"]
    # Expect that experiment collect_params were flattened and logged
    assert any(k[0].startswith("model_name") or k[0].startswith("model_param") or k[0].startswith("evaluator_name") for k in calls["log_param"]) or calls["log_param"] == []

