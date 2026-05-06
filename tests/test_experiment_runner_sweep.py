# tests/test_experiment_runner_sweep.py
from experiments.runner import ExperimentRunner


def test_expand_simple_sweep():
    runner = ExperimentRunner(mlflow_enabled=False)
    experiments = [
        {
            "run_name": "sweep_test",
            "sweep": {
                "params.model_params.nr_topics": [5, 10],
                "params.model_params.min_topic_size": [5, 10]
            },
            "params": {
                "model_params": {}
            }
        }
    ]

    concrete = runner.expand_sweeps(experiments)
    assert len(concrete) == 4
    vals = sorted([c['params']['model_params']['nr_topics'] for c in concrete])
    assert vals == [5,5,10,10]
