# experiments\__init__.py
from .classification_experiment import ClassificationExperiment
from .clustering_experiment import ClusteringExperiment
from .factory import ExperimentFactory

from . import classification_experiment
from . import clustering_experiment

ExperimentFactory.register_experiment("classification", ClassificationExperiment)
ExperimentFactory.register_experiment("clustering", ClusteringExperiment)

__all__ = [
    "ExperimentFactory",
    "ClassificationExperiment",
    "ClusteringExperiment",
]

