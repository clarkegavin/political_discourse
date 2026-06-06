import itertools
from experiments.runner import ExperimentRunner


def _get_values_from_path(cfg, path):
    parts = path.split('.')
    cur = cfg
    for p in parts:
        cur = cur.get(p)
    return cur


def test_flat_sweep():
    runner = ExperimentRunner(mlflow_enabled=False)
    exp = {
        'run_name': 'flat',
        'sweep': {
            'params.model_params.clusterer.params.min_cluster_size': [5, 10]
        }
    }
    out = runner.expand_sweeps([exp])
    assert len(out) == 2
    vals = sorted([_get_values_from_path(o, 'params.model_params.clusterer.params.min_cluster_size') for o in out])
    assert vals == [5, 10]


def test_nested_umap_sweep():
    runner = ExperimentRunner(mlflow_enabled=False)
    exp = {
        'run_name': 'umap',
        'sweep': {
            'params.model_params.dimensionality_reduction_model': [
                {
                    'name': 'umap',
                    'params': {
                        'n_neighbors': [15, 30],
                        'min_dist': [0.0, 0.1]
                    }
                }
            ]
        }
    }
    out = runner.expand_sweeps([exp])
    # should expand to 4 variants
    assert len(out) == 4
    combos = set()
    for o in out:
        m = _get_values_from_path(o, 'params.model_params.dimensionality_reduction_model')
        assert isinstance(m, dict)
        combos.add((m['params']['n_neighbors'], m['params']['min_dist']))
    assert combos == {(15, 0.0), (15, 0.1), (30, 0.0), (30, 0.1)}


def test_nested_pca_sweep():
    runner = ExperimentRunner(mlflow_enabled=False)
    exp = {
        'run_name': 'pca',
        'sweep': {
            'params.model_params.dimensionality_reduction_model': [
                {
                    'name': 'pca',
                    'params': {
                        'n_components': [5, 10]
                    }
                }
            ]
        }
    }
    out = runner.expand_sweeps([exp])
    assert len(out) == 2
    vals = sorted([_get_values_from_path(o, 'params.model_params.dimensionality_reduction_model')['params']['n_components'] for o in out])
    assert vals == [5, 10]


def test_mixed_sweeps():
    runner = ExperimentRunner(mlflow_enabled=False)
    exp = {
        'run_name': 'mixed',
        'sweep': {
            'params.model_params.clusterer.params.min_cluster_size': [5, 10],
            'params.model_params.dimensionality_reduction_model': [
                {
                    'name': 'umap',
                    'params': {
                        'n_neighbors': [15, 30],
                        'min_dist': [0.0, 0.1]
                    }
                },
                {
                    'name': 'pca',
                    'params': {
                        'n_components': [5, 10]
                    }
                }
            ]
        }
    }
    out = runner.expand_sweeps([exp])
    # cluster sizes 2 * (umap 4 + pca 2) = 12
    assert len(out) == 12
    # verify distribution
    cluster_vals = sorted([_get_values_from_path(o, 'params.model_params.clusterer.params.min_cluster_size') for o in out])
    assert cluster_vals.count(5) == 6 and cluster_vals.count(10) == 6


def test_backward_compatibility_mixed_scalar_and_nested():
    runner = ExperimentRunner(mlflow_enabled=False)
    exp = {
        'run_name': 'mixed2',
        'sweep': {
            'params.some_param': ['a', 'b'],
            'params.model_params.dimensionality_reduction_model': [
                {
                    'name': 'umap',
                    'params': {
                        'n_neighbors': [7, 8]
                    }
                }
            ]
        }
    }
    out = runner.expand_sweeps([exp])
    # 2 * 2 = 4
    assert len(out) == 4
    # ensure some_param values present
    some_vals = sorted([o['params']['some_param'] for o in out])
    assert some_vals == ['a', 'a', 'b', 'b']

