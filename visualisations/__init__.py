#visualisations/__init__.py
from .bar_chart import BarChart
from .cluster_plotter import ClusterPlotter
from .confusion_matrix_chart import ConfusionMatrixChart
from .word_cloud import WordCloudChart
from .factory import VisualisationFactory
from .base import Visualisation
from .correlation_matrix import CorrelationMatrix
from .histogram import Histogram
from .boxplot import BoxPlot
from .pair_scatter import PairScatter
from .topic_wordclouds import TopicWordClouds
from .topic_distribution import TopicDistribution
from .intertopic_distance import IntertopicDistance
from .sankey import Sankey
from .zipf_plot import ZipfPlot

# Register visualisations

VisualisationFactory.register_visualisation("confusion_matrix", ConfusionMatrixChart)
VisualisationFactory.register_visualisation("bar_chart", BarChart)
VisualisationFactory.register_visualisation("word_cloud", WordCloudChart)
VisualisationFactory.register_visualisation("cluster_plot", ClusterPlotter)
VisualisationFactory.register_visualisation("correlation_matrix", CorrelationMatrix)
VisualisationFactory.register_visualisation("dython_correlation_matrix", CorrelationMatrix)  # alias
VisualisationFactory.register_visualisation("histogram", Histogram)
VisualisationFactory.register_visualisation("boxplot", BoxPlot)
VisualisationFactory.register_visualisation("pair_scatter", PairScatter)
VisualisationFactory.register_visualisation("topic_wordclouds", TopicWordClouds)
VisualisationFactory.register_visualisation("topic_distribution", TopicDistribution)
VisualisationFactory.register_visualisation("intertopic_distance", IntertopicDistance)
VisualisationFactory.register_visualisation("sankey", Sankey)
VisualisationFactory.register_visualisation("zipf_plot", ZipfPlot)


__all__ = [
    "ConfusionMatrixChart",
    "VisualisationFactory",
    "Visualisation",
    "BarChart",
    "WordCloudChart",
    "ClusterPlotter",
    "CorrelationMatrix",
    "Histogram",
    "BoxPlot",
    "PairScatter",
    "TopicWordClouds",
    "Sankey",
    "TopicDistribution",
    "ZipfPlot"
]
