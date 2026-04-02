#evaluators/__init__.py
from .factory import EvaluatorFactory
from . import classification_evaluator
from . import clustering_evaluators
from .clustering_profile_evaluator import ClusterProfileEvaluator
from .clustering_quality_evaluator import ClusteringQualityEvaluator
from .topic_modeling_evaluator import TopicModelingEvaluator

# Register evaluators
EvaluatorFactory.register_evaluator("classification", classification_evaluator.ClassificationEvaluator)
EvaluatorFactory.register_evaluator("clustering", clustering_evaluators.ClusteringEvaluator)
EvaluatorFactory.register_evaluator("clustering_quality", ClusteringQualityEvaluator)
EvaluatorFactory.register_evaluator("cluster_profile", ClusterProfileEvaluator)
EvaluatorFactory.register_evaluator("topic_modeling", TopicModelingEvaluator)


__all__ = [
    "EvaluatorFactory",
    "classification_evaluator",
    "clustering_evaluators",
    "topic_modeling_evaluator",
]
