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


EDAFactory.register_eda("class_balance", ClassBalanceEDA)
EDAFactory.register_eda("wordcloud_global", lambda: WordCloudEDA(per_class=False))
EDAFactory.register_eda("wordcloud_by_class", lambda: WordCloudEDA(per_class=True))
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
    "ShannonEntropyEDA"

]
