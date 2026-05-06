# tests/test_experiment_factory_from_config.py
from experiments.factory import ExperimentFactory


class DummyExp:
    def __init__(self, name=None, X=None, foo=None, global_config=None, **kwargs):
        self.name = name
        self.X = X
        self.foo = foo
        self.global_config = global_config

    def run(self):
        return {"df": None, "metadata": {"foo": self.foo}}


def test_build_experiment_from_config():
    ExperimentFactory.register_experiment('dummy', DummyExp)

    cfg = {
        "name": "r1",
        "params": {"foo": 123},
        "extra": "value"
    }

    exp = ExperimentFactory.build_experiment_from_config('dummy', cfg, X='XDATA', global_config={'g': 1})
    assert isinstance(exp, DummyExp)
    assert exp.X == 'XDATA'
    assert exp.foo == 123
    assert exp.global_config == {'g': 1}

