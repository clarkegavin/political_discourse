# orchestrator/pipeline_orchestrator.py
from typing import List, Optional, Dict
from logs.logger import get_logger
import pandas as pd

from pipelines import TargetFeaturePipeline, DataSplitterPipeline, FeatureEncoderPipeline, FilterPipeline, \
    ExperimentPipeline, DataExtractorPipeline


# from pipelines.experiment_pipeline import ExperimentPipeline


# class FeatureEncoderPipeline:
#     pass


class PipelineOrchestrator:
    def __init__(self, pipelines: List, max_retries: int = 3, parallel: bool = False):
        self.logger = get_logger(self.__class__.__name__)
        self.pipelines = pipelines
        self.max_retries = max_retries
        self.parallel = parallel



    def run_pipeline(self, pipeline, data: Optional[pd.DataFrame] = None, extra: Optional[Dict] = None):
        """Run a single pipeline with retry logic."""
        attempt = 0
        while attempt < self.max_retries:
            try:
                self.logger.info(f"Running pipeline: {pipeline.__class__.__name__}, attempt {attempt+1}")

                if extra is None:
                    extra = {}

                # Allow TargetFeaturePipeline to get y_train/y_test explicitly
                if isinstance(pipeline, TargetFeaturePipeline):
                    result = pipeline.execute(
                        y=extra["y"],
                        fit=extra.get("fit", True)
                    )
                elif isinstance(pipeline, ExperimentPipeline):
                    self.logger.info("Executing ExperimentPipeline with train/test data")
                    # check if target_encoder is in extra
                    self.logger.info(f"Target encoder being passed in extra: {'target_encoder' in extra}")
                    result = pipeline.execute(
                        **extra
                    )
                else:
                    self.logger.info(f"Executing general pipeline: {pipeline.__class__.__name__} with data shape: {data.shape if data is not None else 'N/A'}")
                    result = pipeline.execute(data)
                    self.logger.info(f"Pipeline {pipeline.__class__.__name__} output shape: {result.shape if isinstance(result, pd.DataFrame) else 'N/A'}")
                    self.logger.info(f"Result type: {type(result)}")
                    # log result features if result is a DataFrame
                    # if isinstance(result, pd.DataFrame):
                    #     self.logger.info(f"Result columns: {result.columns.tolist()}")


                self.logger.info(f"Pipeline {pipeline.__class__.__name__} completed successfully")
                return result
            except Exception as e:
                attempt += 1
                self.logger.error(f"Pipeline {pipeline.__class__.__name__} failed on attempt {attempt}: {e}")

        self.logger.error(f"Pipeline {pipeline.__class__.__name__} failed after {self.max_retries} attempts")
        return None

    def run(self, data: Optional[pd.DataFrame] = None, target_column: str = None):
        """Run all pipelines sequentially."""

        self.logger.info("Starting orchestrator run")
        X_train = X_test = y_train = y_test = None
        current_data = data
        if target_encoder := None:
            self.logger.info("No target encoder at start of orchestration")

        for pipeline in self.pipelines:
            if isinstance(pipeline, DataSplitterPipeline):
                # Split data into train/test
                splits = pipeline.execute(current_data)
                X_train, X_test = splits["X_train"], splits["X_test"]
                y_train, y_test = splits["y_train"], splits["y_test"]
            elif isinstance(pipeline, TargetFeaturePipeline):
                # Fit on training target, transform test target
                y_train_encoded = self.run_pipeline(pipeline, extra={"y": y_train, "fit": True})
                y_test_encoded = self.run_pipeline(pipeline, extra={"y": y_test, "fit": False})
                # Replace y_train/y_test for downstream pipelines if needed
                y_train, y_test = y_train_encoded, y_test_encoded
                # store the encoder
                target_encoder = pipeline.encoder

            elif isinstance(pipeline, FeatureEncoderPipeline):
                # Note: this is poor design, as FeatureEncoderPipeline should not depend on DataSplitterPipeline
                if X_train is None or X_test is None:
                    self.logger.warning("DataSplitterPipeline must run before FeatureEncoderPipeline for supervised models")
                    # raise RuntimeError(
                    #     "FeatureEncoderPipeline cannot run before DataSplitterPipeline"
                    # )
                    current_data = self.run_pipeline(pipeline, data=current_data)
                else:
                    self.logger.warning("DataSplitterPipeline has run; applying FeatureEncoderPipeline to train/test sets")
                    X_train = self.run_pipeline(pipeline, data=X_train, extra={"fit": True})
                    X_test = self.run_pipeline(pipeline, data=X_test, extra={"fit": False})
            elif isinstance(pipeline, ExperimentPipeline):
                # Pass train/test data explicitly
                self.logger.info(f"Target encoder being passed to ExperimentPipeline: {target_encoder is not None}")
                self.run_pipeline(pipeline, extra={
                    "X_train": X_train,
                    "X_test": X_test,
                    "y_train": y_train,
                    "y_test": y_test,
                    "target_encoder": target_encoder,
                    #"global_config": self.global_config
                })
            else:
                # Other pipelines that operate on the full dataset
                self.logger.info(f"Running general pipeline: {pipeline.__class__.__name__}")

                if current_data is None:
                    if pipeline.__class__ != DataExtractorPipeline:
                        self.logger.error(f"No data available for pipeline {pipeline.__class__.__name__}")
                else:
                    self.logger.info(
                        f"Current data shape before {pipeline.__class__.__name__}: {current_data.shape}"
                    )

                current_data = self.run_pipeline(pipeline, data=current_data)
                #self.logger.info(f"Data shape after {pipeline.__class__.__name__}: {current_data.shape}")

        self.logger.info("Pipeline orchestration complete")
        return X_train, X_test, y_train, y_test
