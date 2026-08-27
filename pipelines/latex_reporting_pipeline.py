import os
from typing import Optional, Any

from .base import Pipeline
from logs.logger import get_logger

from reporting.factory import ReportingFactory


class LatexReportingPipeline(Pipeline):

    def __init__(
        self,
        output_path: str,
        steps=None,
        **kwargs
    ):

        self.logger = get_logger(self.__class__.__name__)

        self.logger.info(
            "Initializing LaTeX Reporting Pipeline"
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
            "Executing LaTeX Reporting Pipeline..."
        )

        if data is None:
            raise ValueError(
                "LatexReportingPipeline requires input data."
            )

        self.logger.info(
            f"Input dataframe contains columns: "
            f"{list(data.columns)}"
        )

        outputs = {}

        self.logger.info(f"Data shape: {data.shape}")

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
                data=data,
                output_path=self.output_path,
                **step_params
            )

            outputs[name] = results

        self.logger.info(
            "LaTeX Reporting Pipeline complete."
        )

        return data