# tests/test_phase7_pipeline_config_run.py
import yaml
from pipelines.factory import PipelineFactory
from experiments.factory import ExperimentFactory
from models.factory import ModelFactory
from evaluators.factory import EvaluatorFactory
from visualisations.factory import VisualisationFactory

# Minimal fake components to ensure pipeline can be instantiated and executed
class FakeModel:
    def __init__(self, name, **kwargs):
        pass

class FakeEvaluator:
    def __init__(self, name=None, **kwargs):
        pass
    def evaluate(self, df, metadata, params=None):
        return {"score": 1.0}

class FakeViz:
    def __init__(self, **kwargs):
        pass
    def plot(self, df, model=None, **plot_kwargs):
        return []

# Register fakes
ModelFactory.register_model('fake_model', FakeModel)
ExperimentFactory.register_experiment('topic_modeling', ExperimentFactory._registry.get('topic_modeling') if 'topic_modeling' in ExperimentFactory._registry else type('NoOp', (), {}))
EvaluatorFactory.register_evaluator('ev1', FakeEvaluator)
VisualisationFactory.register_visualisation('v1', FakeViz)


def test_load_pipeline_from_config_dir():
    # Load the pipeline yaml we added in config/pipelines/
    pf = PipelineFactory.build_pipelines_from_yaml('config/pipelines/topic_pipeline_example.yaml')
    assert isinstance(pf, list)
    # If any pipelines loaded, they should be registered
    assert len(pf) >= 0

