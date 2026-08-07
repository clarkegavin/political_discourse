import os
import logging
from typing import Optional, Any

from .base import Pipeline
from logs.logger import get_logger

from reporting.factory import ReportingFactory
from reporting.reporting_data_builder import ReportingDataBuilder

class ThesisReportingPipeline(Pipeline):

    def __init__(
        self,
        output_path: str,
        steps=None,
        **kwargs
    ):

        self.logger = get_logger(self.__class__.__name__)

        self.logger.info(
            "Initializing Thesis Reporting Pipeline"
        )

        self.reporting_data_builder = ReportingDataBuilder(
            **kwargs.get("data_builder", {})
        )
        self.output_path = output_path
        self.reporting_steps = steps or []

        os.makedirs(
            self.output_path,
            exist_ok=True
        )

        self.kwargs = kwargs

    def execute(
            self,
            data: Optional[Any] = None
    ):

        self.logger.info(
            "Executing Thesis Reporting Pipeline..."
        )

        # Preserve incoming pipeline data
        original_data = data

        if original_data is not None:
            self.logger.info(
                f"Input data contains columns: {list(original_data.columns)}"
            )

        # Build reporting dataframe independently
        self.logger.info(
            "Building reporting dataframe from MLflow"
        )

        reporting_data = self.reporting_data_builder.build()

        self.logger.info(f"Reporting dataframe contains columns: {list(reporting_data.columns)}")

        outputs = {}

        for step in self.reporting_steps:
            name = step["name"]

            self.logger.info(
                f"Running reporting step '{name}'"
            )

            report_component = ReportingFactory.get_report(name)

            step_params = step.get(
                "params",
                {}
            ) or {}

            results = report_component.run(
                data=reporting_data,
                output_path=self.output_path,
                **step_params
            )

            outputs[name] = results

        self.logger.info(
            "Thesis Reporting Pipeline complete."
        )

        # Pipeline contract:
        # return original input data unchanged
        return original_data