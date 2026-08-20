from .factory import ReportingFactory
from .latex_summary_table import LatexSummaryTable
from .latex_table import LatexTable
from .mlflow_reader import MLflowReader
from .charts.grouped_metric_chart import GroupedMetricChart
from .charts.metric_heatmap import MetricHeatmap
from .charts.metric_line_chart import MetricLineChart
from .charts.metric_strip_chart import MetricStripChart
from .charts.component_comparison_chart import ComponentComparisonChart

ReportingFactory.register_report("latex_summary_table", LatexSummaryTable)
ReportingFactory.register_report("mlflow_reader_report", MLflowReader)
ReportingFactory.register_report("grouped_metric_chart", GroupedMetricChart)
ReportingFactory.register_report("metric_heatmap", MetricHeatmap)
ReportingFactory.register_report("metric_line_chart", MetricLineChart)
ReportingFactory.register_report("metric_strip_chart", MetricStripChart)
ReportingFactory.register_report("component_comparison_chart", ComponentComparisonChart)
ReportingFactory.register_report("latex_table", LatexTable)

