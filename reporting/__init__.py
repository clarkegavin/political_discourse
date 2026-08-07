from .factory import ReportingFactory
from .latex_summary_table import LatexSummaryTable
from .mlflow_reader import MLflowReader
from .charts.grouped_metric_chart import GroupedMetricChart

ReportingFactory.register_report("latex_summary_table", LatexSummaryTable())
ReportingFactory.register_report("mlflow_reader_report", MLflowReader())
ReportingFactory.register_report("grouped_metric_chart", GroupedMetricChart())

