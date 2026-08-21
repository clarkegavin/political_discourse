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
from .choropleth import ChoroplethVisualisation
from .bubble_geo_map import BubbleGeoMapVisualisation
from .bubble_geo_map_interactive import InteractiveBubbleGeoMap
from .word_cloud_eda_vis import WordCloudVisualisation
from .area_plot import AreaPlot
from .line_plot import LinePlot
from .language_distribution import LanguageDistributionVisualisation
from .language_by_column import LanguageByColumnVisualisation
from .topic_barchart import TopicBarChart
from .topic_documents import TopicDocuments
from .topic_heatmap import TopicHeatmap
from .topic_hierarchy import TopicHierarchy
from .topic_term_rank import TopicTermRank
from .bertopic_visualisation import BERTopicVisualisation
from .topics_over_time import TopicsOverTime
from .topics_per_class import TopicsPerClass
from .grouped_bar_chart import GroupedBarChart
from .metric_heatmap import MetricHeatmap
from .metric_line_chart import MetricLineChart
from .metric_strip_plot import MetricStripPlot
from .metric_bar_chart import MetricBarChart
from .topic_tree import TopicTree
from .topic_hierarchical_documents import HierarchicalDocuments
from .topic_approximate_distribution import ApproximateDistribution
from .topic_attribute_subplot import TopicAttributeSubplot
from .entity_topic_profile_bar_chart import EntityTopicProfileBarChart
from .similarity_histogram import SimilarityHistogram
from .similarity_line_plot import SimilarityLinePlot

# Register visualisations

VisualisationFactory.register_visualisation("confusion_matrix", ConfusionMatrixChart)
VisualisationFactory.register_visualisation("bar_chart", BarChart)
VisualisationFactory.register_visualisation("barchart", BarChart)
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
VisualisationFactory.register_visualisation("choropleth", ChoroplethVisualisation)
VisualisationFactory.register_visualisation("bubble_geo_map", BubbleGeoMapVisualisation)
VisualisationFactory.register_visualisation("interactive_bubble_geo_map", InteractiveBubbleGeoMap)
VisualisationFactory.register_visualisation("word_cloud_eda", WordCloudVisualisation)
VisualisationFactory.register_visualisation("eda_wordcloud", WordCloudVisualisation)  # alias
VisualisationFactory.register_visualisation('line_plot', LinePlot)
VisualisationFactory.register_visualisation('area_plot', AreaPlot)
VisualisationFactory.register_visualisation("language_distribution",  LanguageDistributionVisualisation)
VisualisationFactory.register_visualisation("language_by_column",   LanguageByColumnVisualisation)
VisualisationFactory.register_visualisation("topic_barchart", TopicBarChart)
VisualisationFactory.register_visualisation("topic_barcharts", TopicBarChart)  # alias
VisualisationFactory.register_visualisation("topic_documents", TopicDocuments)
VisualisationFactory.register_visualisation("topic_heatmap", TopicHeatmap)
VisualisationFactory.register_visualisation("topic_hierarchy", TopicHierarchy)
VisualisationFactory.register_visualisation("topic_term_rank", TopicTermRank)
VisualisationFactory.register_visualisation("topics_over_time", TopicsOverTime)
VisualisationFactory.register_visualisation("topics_per_class", TopicsPerClass)
VisualisationFactory.register_visualisation("grouped_bar_chart", GroupedBarChart)
VisualisationFactory.register_visualisation("metric_heatmap", MetricHeatmap)
VisualisationFactory.register_visualisation("metric_line_chart", MetricLineChart)
VisualisationFactory.register_visualisation("metric_strip_plot", MetricStripPlot)
VisualisationFactory.register_visualisation("metric_bar_chart", MetricBarChart)
VisualisationFactory.register_visualisation("topic_tree", TopicTree)
VisualisationFactory.register_visualisation("hierarchical_documents", HierarchicalDocuments)
VisualisationFactory.register_visualisation("approximate_distribution", ApproximateDistribution)
VisualisationFactory.register_visualisation("topic_attribute_subplot", TopicAttributeSubplot)
VisualisationFactory.register_visualisation("entity_topic_profile_bar_chart", EntityTopicProfileBarChart)
VisualisationFactory.register_visualisation("similarity_histogram", SimilarityHistogram)
VisualisationFactory.register_visualisation("similarity_line_plot", SimilarityLinePlot)


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
    "ZipfPlot",
    "ChoroplethVisualisation",
    "BubbleGeoMapVisualisation",
    "InteractiveBubbleGeoMap",
    "WordCloudVisualisation",
    "AreaPlot",
    "LinePlot",
    "LanguageDistributionVisualisation",
    "LanguageByColumnVisualisation",
    "TopicBarChart",
    "TopicDocuments",
    "TopicHeatmap",
    "TopicHierarchy",
    "TopicTermRank",
    "BERTopicVisualisation",
    "TopicsOverTime",
    "TopicsPerClass",
    "GroupedBarChart",
    "MetricHeatmap",
    "MetricLineChart",
    "MetricStripPlot",
    "MetricBarChart",
    "TopicTree",
    "HierarchicalDocuments",
    "ApproximateDistribution",
    "TopicAttributeSubplot",
    "EntityTopicProfileBarChart",
    "SimilarityHistogram",
    "SimilarityLinePlot",
]
