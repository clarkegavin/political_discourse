# tests/test_experiment_runner_eval_viz_unit.py
from experiments.runner import ExperimentRunner
from experiments.factory import ExperimentFactory


class DummyExp:
    def __init__(self, name='de', X=None, **kwargs):
        self.name = name
        self.X = X

    def run(self):
        return {"df": None, "metadata": {"evaluator_name": "ev1", "visualisations": [{"name": "v1", "init": {}, "plot": {}}]}, "artifacts": []}

    def collect_params(self):
        return {"foo": "bar"}


def test_runner_calls_eval_and_viz(monkeypatch):
    ExperimentFactory.register_experiment('dummy_exp', DummyExp)

    calls = {"eval": 0, "viz": 0, "mlf_start": 0, "mlf_end": 0, "mlf_log_param": []}

    import experiments.runner as er

    # Monkeypatch EvaluationRunner.evaluate
    def fake_eval(self, result, cfg):
        calls['eval'] += 1
        return {"metrics": {"m1": 0.5}, "artifacts": ["/tmp/art1.png"]}

    monkeypatch.setattr(er.EvaluationRunner, 'evaluate', fake_eval)

    # Monkeypatch VisualisationRunner.render
    def fake_viz(self, result, viz_cfg, model=None):
        calls['viz'] += 1
        return ["/tmp/viz1.png"]

    monkeypatch.setattr(er.VisualisationRunner, 'render', fake_viz)

    # Monkeypatch mlflow
    def fake_set_experiment(name):
        pass
    def fake_start_run(**kwargs):
        calls['mlf_start'] += 1
    def fake_log_param(k, v):
        calls['mlf_log_param'].append((k, v))
    def fake_log_metric(k, v):
        pass
    def fake_log_artifact(p):
        pass
    def fake_end_run():
        calls['mlf_end'] += 1

    monkeypatch.setattr(er.mlflow, 'set_experiment', fake_set_experiment)
    monkeypatch.setattr(er.mlflow, 'start_run', fake_start_run)
    monkeypatch.setattr(er.mlflow, 'log_param', fake_log_param)
    monkeypatch.setattr(er.mlflow, 'log_metric', fake_log_metric)
    monkeypatch.setattr(er.mlflow, 'log_artifact', fake_log_artifact)
    monkeypatch.setattr(er.mlflow, 'end_run', fake_end_run)

    runner = ExperimentRunner(mlflow_enabled=True)
    res = runner.run_experiments('dummy_exp', [{"run_name": "r1", "params": {}}])

    assert calls['eval'] == 1
    assert calls['viz'] == 1
    assert calls['mlf_start'] == 1
    assert calls['mlf_end'] == 1
    # ensure collect_params were logged
    assert any(k[0] == 'foo' for k in calls['mlf_log_param'])

