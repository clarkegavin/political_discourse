# tests/test_experiment_runner_sweep_overrides.py
from experiments.runner import ExperimentRunner


def test_expand_sweep_and_overrides():
    runner = ExperimentRunner(mlflow_enabled=False)
    experiments = [
        {
            "run_name": "run_{model_param}",
            "sweep": {
                "params.model_param": ["a", "b"]
            },
            "overrides": [
                {"params.extra": 1},
                {"params.extra": 2}
            ],
            "params": {"model_param": "x"}
        }
    ]

    concrete = runner.expand_sweeps(experiments)
    # after sweep we should have 2 combos
    assert len(concrete) == 2

    applied = runner._apply_overrides(concrete)
    # overrides expand each sweep by 2 -> 4
    assert len(applied) == 4

    # run names should be templated when formatting
    formatted = [runner._format_run_name(exp.get('run_name', ''), exp.get('params', {})) for exp in applied]
    assert 'run_a' in formatted and 'run_b' in formatted

