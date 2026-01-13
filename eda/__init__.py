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
]
