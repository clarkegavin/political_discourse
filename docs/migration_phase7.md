# Migration Phase 7

Phase 7 focuses on backfilling config examples, documentation and a migration checklist to support moving from `config/pipelines.yaml` to the layered config layout. This document includes examples and a minimal migration recipe.

Goals
- Provide example config files under `config/experiments/` and `config/pipelines/`.
- Document the new experiment runner usage and the keys supported in experiment config: `params`, `sweep`, `overrides`, `run_name`, `retries`, `max_runs`, `visualisations`.
- Provide a migration checklist and sample commands to validate.

Example experiment config (see `config/experiments/topic_example.yaml`)
- Top-level keys: `name`, `model_name`, `evaluator_name`, `mlflow_experiment`, `visualisations`, `params`, `sweep`.
- `params` maps to constructor kwargs for the experiment.

Example pipeline config (see `config/pipelines/topic_pipeline_example.yaml`)
- pipelines: list of pipeline entries.
- Each pipeline entry includes `type` (module.class), `params` matching pipeline __init__ args.
- TopicModelingPipeline params include `model_name`, `evaluator_name`, and `experiments` (a list of experiment configs or refs).

Migration checklist
1. Create experiment config files under `config/experiments/` for each experiment you run.
2. Update `config/pipelines/*.yaml` to reference experiments by embedding experiment configs or pointing to files.
3. Update `PipelineFactory.build_pipelines_from_yaml` to support dirs (planned in Phase 8), until then keep legacy `config/pipelines.yaml` but add new files for examples.
4. Run unit/integration tests to ensure existing pipelines still work.

Validation steps (local)
- Run the new Phase 7 integration test added to `tests/` to exercise the new configs.
- Optional: run `main.py` after updating `yaml_path` to new pipeline file to perform an end-to-end run.

