from .factory import ReportingFactory
from .latex_summary_table import LatexSummaryTable
from .mlflow_reader import MLflowReader
from .charts.grouped_metric_chart import GroupedMetricChart
from .charts.metric_heatmap import MetricHeatmap
from .charts.metric_line_chart import MetricLineChart

ReportingFactory.register_report("latex_summary_table", LatexSummaryTable)
ReportingFactory.register_report("mlflow_reader_report", MLflowReader)
ReportingFactory.register_report("grouped_metric_chart", GroupedMetricChart)
ReportingFactory.register_report("metric_heatmap", MetricHeatmap)
ReportingFactory.register_report("metric_line_chart", MetricLineChart)

