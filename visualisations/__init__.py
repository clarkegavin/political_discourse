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
VisualisationFactory.register_visualisation(
    "language_distribution",
    LanguageDistributionVisualisation
)
VisualisationFactory.register_visualisation(
    "language_by_column",
    LanguageByColumnVisualisation
)

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
    "LanguageByColumnVisualisation"
]
