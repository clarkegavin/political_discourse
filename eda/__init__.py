#eda/__init__.py
from .factory import EDAFactory
from .class_balance_eda import ClassBalanceEDA
from .wordcloud_eda import WordCloudEDA
from .duplicate_check_eda import DuplicateCheckEDA
from .describe_info_eda import DescribeInfoEDA
from .info_eda import InfoEDA
from .dython_correlation_eda import DythonCorrelationEDA
from .boxplot_eda import BoxPlotEDA
from .pair_scatter_eda import PairScatterEDA
from .scatter_eda import ScatterPlotEDA
from .document_length_eda import DocumentLengthEDA
from .consolidation_eda import ConsolidationEDA
from .term_frequency_eda import TermFrequencyEDA
from .sankey_flow_eda import SankeyFlowEDA
from .missing_values_eda import MissingValuesEDA
from .tokenize_text_eda import TokenizeTextEDA
from .shannon_entropy_eda import ShannonEntropyEDA
from .geomap_eda import GeoMapEDA
from .temporal_analysis_eda import TemporalAnalysisEDA
from .language_detection_eda import LanguageDetectionEDA
from .reply_chain_eda import ReplyChainAnalysisEDA
from .group_count_eda import GroupCountDistributionEDA

EDAFactory.register_eda("class_balance", ClassBalanceEDA)
EDAFactory.register_eda("wordcloud", WordCloudEDA)
EDAFactory.register_eda("duplicate_check", DuplicateCheckEDA)
EDAFactory.register_eda("describe_info", DescribeInfoEDA)
EDAFactory.register_eda("info", InfoEDA)
EDAFactory.register_eda("dython_correlation_matrix", DythonCorrelationEDA)
EDAFactory.register_eda("correlation_matrix", DythonCorrelationEDA)
EDAFactory.register_eda("boxplots", BoxPlotEDA)
EDAFactory.register_eda("pair_scatter", PairScatterEDA)
EDAFactory.register_eda("scatter_plot", ScatterPlotEDA)
EDAFactory.register_eda("document_length", DocumentLengthEDA)
EDAFactory.register_eda("consolidation", ConsolidationEDA)
EDAFactory.register_eda("term_frequency", TermFrequencyEDA)
EDAFactory.register_eda("sankey_flow", SankeyFlowEDA)
EDAFactory.register_eda("missing_values", MissingValuesEDA)
EDAFactory.register_eda("tokenize_text", TokenizeTextEDA)
EDAFactory.register_eda("shannon_entropy", ShannonEntropyEDA)
EDAFactory.register_eda("geomap", GeoMapEDA)
EDAFactory.register_eda('temporal_analysis', TemporalAnalysisEDA)
EDAFactory.register_eda(
    "language_detection",
    LanguageDetectionEDA
)
EDAFactory.register_eda("reply_chain", ReplyChainAnalysisEDA)
EDAFactory.register_eda("group_count_distribution", GroupCountDistributionEDA)

__all__ = [
    "EDAFactory",
    "ClassBalanceEDA",
    "WordCloudEDA",
    "DuplicateCheckEDA",
    "DescribeInfoEDA",
    "InfoEDA",
    "DythonCorrelationEDA",
    "BoxPlotEDA",
    "PairScatterEDA",
    "ScatterPlotEDA",
    "DocumentLengthEDA",
    "ConsolidationEDA",
    "TermFrequencyEDA",
    "SankeyFlowEDA",
    "MissingValuesEDA",
    "TokenizeTextEDA",
    "ShannonEntropyEDA",
    "GeoMapEDA",
    "LanguageDetectionEDA",
    "ReplyChainAnalysisEDA",
    "TemporalAnalysisEDA",
    "GroupCountDistributionEDA"
]
