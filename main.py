# main.py
from dotenv import load_dotenv
from orchestrator.pipeline_orchestrator import PipelineOrchestrator
from pipelines.factory import PipelineFactory
from logs.logger import get_logger
from pathlib import Path
from experiments.runner import ExperimentRunner
import os

load_dotenv()

def main():
    # Prefer directory-based config, fall back to legacy single-file YAML
    dir_path = Path("config/pipelines_boards")
    file_path = Path("config/pipelines.yaml")
    os.environ["LOGNAME"]  = "Gavin"  # Set LOGNAME for consistent logger naming

    logger = get_logger("Main")

    if dir_path.exists() and dir_path.is_dir():
        cfg_source = str(dir_path)
        logger.info(f"Loading pipelines from directory: {cfg_source}")
        pipelines = PipelineFactory.build_pipelines_from_path(cfg_source)
    elif file_path.exists() and file_path.is_file():
        cfg_source = str(file_path)
        logger.info(f"Loading pipelines from legacy YAML: {cfg_source}")
        pipelines = PipelineFactory.build_pipelines_from_path(cfg_source)
    else:
        logger.error("No pipeline configuration found at 'config/pipelines/' or 'config/pipelines.yaml'")
        return

    logger.info(f"Loaded {len(pipelines)} pipelines from {cfg_source}")
    logger.info("Pipelines order:")



    for i, pipeline in enumerate(pipelines):
        logger.info(f"  {i+1}. {pipeline.__class__.__name__}")


    # Ensure ExperimentRunner is available and make it accessible to the orchestrator
    exp_runner = ExperimentRunner(mlflow_enabled=True)
    orchestrator = PipelineOrchestrator(pipelines=pipelines, parallel=False, max_retries=3)
    # attach runner to orchestrator for pipelines that may use it
    setattr(orchestrator, "experiment_runner", exp_runner)
    logger.info("Attached ExperimentRunner to orchestrator")

    data = None  # If your first pipeline extracts data, this can be None

    X_train, X_test, y_train, y_test = orchestrator.run(data=data, target_column="Genre")


if __name__ == "__main__":
    main()