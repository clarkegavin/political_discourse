# experiments\__init__.py
from .classification_experiment import ClassificationExperiment
from .clustering_experiment import ClusteringExperiment
from .factory import ExperimentFactory
from .topic_modeling_experiment import TopicModelingExperiment

from . import classification_experiment
from . import clustering_experiment

ExperimentFactory.register_experiment("classification", ClassificationExperiment)
ExperimentFactory.register_experiment("clustering", ClusteringExperiment)
ExperimentFactory.register_experiment("topic_modeling", TopicModelingExperiment)

__all__ = [
    "ExperimentFactory",
    "ClassificationExperiment",
    "ClusteringExperiment",
]
