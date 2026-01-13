# pipelines/eda_pipeline.py
import os
import logging
from eda.factory import EDAFactory
from .base import Pipeline
import pandas as pd
from typing import Optional
from logs.logger import get_logger

class EDAPipeline(Pipeline):

    def __init__(self, save_path, steps=None, dataset=None, **kwargs):
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("Initializing EDA Pipeline")
        #super().__init__()

        self.eda_steps = steps or []
        self.save_path = save_path
        self.dataset = dataset or {}
        self.kwargs = kwargs
        self.target = self.kwargs.get("target")
        self.text_field = self.kwargs.get("text_field")

        self.logger.info(f"EDA Target: {self.target}, Text Field: {self.text_field}")
        os.makedirs(self.save_path, exist_ok=True)


    def execute(self, data: Optional[pd.DataFrame] = None):
        self.logger.info("Executing EDAPipeline...")
        if data is None:
            raise ValueError("Data must be provided to EDAPipeline.")

        self.logger.info("Starting EDA Pipeline")

        outputs = {}

        for step in self.eda_steps:
            name = step["name"]
            self.logger.info(f"Running EDA step '{name}'")

            eda_component = EDAFactory.get_eda(name)

            # Forward any step-specific params from the pipeline YAML into the EDA run
            step_params = step.get("params", {}) or {}

            results = eda_component.run(
                data=data,
                target=self.target,
                text_field=self.text_field,
                save_path=self.save_path,
                **step_params,
            )

            outputs[name] = results

        self.logger.info("EDA Pipeline complete.")
        return data
