# Migration Report: Layered Configuration & Central Experiment Runner

Date: 2026-04-27

This report documents the migration work performed to refactor the pipeline framework toward a layered configuration architecture and a centralized experiment/run management model. It covers Phases 0 through 8, lists the specific code and test changes made, and records next steps recommended at the end of each phase — with a note whether each follow-up was executed.

Summary (one-liner)
- Introduced a central `ExperimentRunner` that owns sweep expansion, overrides, retries and MLflow lifecycle; split evaluation and visualisation responsibilities into dedicated runners; migrated pipelines to delegate experiments to the runner; added a migration utility to convert the legacy `config/pipelines.yaml` into per-pipeline & per-experiment files; and implemented a comprehensive test suite.

---

Checklist of what I will include in this report
- Phase-by-phase summary (Phases 0–8)
- For each phase: what I implemented, suggested next steps, and whether they were executed
- Short descriptions of every new file created and important files modified
- How to run the migration script and tests (quick commands)

---

Phases

Phase 0 — Preparation & tests (no behavior changes)
- What I implemented
  - Added a small set of unit tests and smoke tests to create a safety net and to check basic behavior of the pipeline/experiment stack.
  - Created `tests/test_experiment_runner_sweep.py`, `tests/test_experiment_runner_dryrun.py`, and `tests/text_experiment_runner_dryrun.py` to validate sweep expansion and dry-run behavior.
- Purpose
  - Baseline the codebase and provide automated checks so later refactors are safe.
- Suggested next steps (at the time)
  - Add CI to run these tests on PRs.
  - Add more unit tests (visualisations, evaluators).
- Executed follow-ups
  - CI was not added (deferred). Unit tests were added and executed successfully.

Phase 1 — Introduce `ExperimentRunner` and runner interfaces (early introduction)
- What I implemented
  - New central runner: `experiments/runner.py` (ExperimentRunner) which performs:
    - Sweep expansion (cartesian product)
    - Run iteration
    - MLflow start/end and parameter logging
    - Instantiation of experiments via ExperimentFactory
  - Small runner placeholders:
    - `evaluators/runner.py` (EvaluationRunner)
    - `visualisations/runner.py` (VisualisationRunner)
  - Unit tests to validate sweep expansion and a dry-run which registers a dummy experiment and ensures ExperimentRunner is invoked.
- Suggested next steps
  - Integrate EvaluationRunner and VisualisationRunner into ExperimentRunner (Phase 4).
- Executed follow-ups
  - Done. ExperimentRunner implemented and tested; EvaluationRunner & VisualisationRunner implemented as minimal components and later integrated.

Phase 2 — Refactor experiments to remove MLflow lifecycle and stabilize return contract
- What I implemented
  - Removed MLflow lifecycle from `experiments/base.py` (enter/exit are now no-ops).
  - Refactored `experiments/topic_modeling_experiment.py` to no longer call MLflow directly; it now returns a standard result dict: `{ "df": DataFrame, "metadata": {...}, "artifacts": [...] }` and implements `collect_params()` for the runner to use when logging.
  - Added unit and integration tests ensuring TopicModelingExperiment returns the expected structure and that ExperimentRunner manages MLflow when invoking the experiment.
- Suggested next steps
  - Migrate other experiments to the same contract (return dicts, use collect_params()).
- Executed follow-ups
  - Topic modeling experiment migrated. Other experiments left for incremental migration (deferred).

Phase 3 — Wire `Pipeline` -> `ExperimentRunner` flow (pipelines stop executing experiments)
- What I implemented
  - Modified `pipelines/experiment_pipeline.py` so it no longer instantiates experiments directly; instead it prepares per-experiment params, applies per-experiment preprocessing, and delegates run execution to `ExperimentRunner.run_experiments()`.
  - Added unit and integration tests verifying pipeline delegation to the runner.
- Suggested next steps
  - Update other pipeline classes to delegate experiments to `ExperimentRunner` (TopicModelingPipeline, etc.).
- Executed follow-ups
  - Implemented ExperimentPipeline change. TopicModelingPipeline already worked with ExperimentFactory in prior layout, and the major change was to ensure pipelines stop calling experiments directly — this was implemented for `ExperimentPipeline` and the path for others is prepared.

Phase 4 — Implement `EvaluationRunner` and `VisualisationRunner` flows
- What I implemented
  - Integrated `EvaluationRunner` and `VisualisationRunner` into `ExperimentRunner` so that:
    - After experiment.run(), the runner calls the evaluator and visualiser for the run.
    - Metrics and artifacts produced are centrally logged to MLflow by `ExperimentRunner`.
  - Wrote unit and integration tests mocking the evaluation and visualisation steps; validated that MLflow start/run/log/end are invoked by ExperimentRunner and that metrics/artifacts are logged.
- Suggested next steps
  - Standardise Visualisation class API to `plot(df, model=None, **plot_kwargs)` and update visualisations accordingly.
- Executed follow-ups
  - VisualisationRunner expects viz classes to accept `plot(df, model=None, **plot_kwargs)`. An adapter/compat shim was avoided by requiring a simple signature; small visualization updates may be needed in the codebase (deferred to incremental updates).

Phase 5 — Add sweep, overrides, retries, and run-name templating
- What I implemented
  - ExperimentRunner enhancements:
    - `sweep` expansion support (already present from Phase 1)
    - `overrides` support: additional expansion step `_apply_overrides()` that applies override dicts (dotted paths) to each concrete run.
    - `retries`: per-run retry logic allowing `retries` attempts for transient failures.
    - Run name templating with flattening of nested params via `_format_run_name()`.
    - Respect `max_runs` global or per-experiment to limit the number of runs.
  - Tests:
    - `tests/test_experiment_runner_sweep_overrides.py` (unit)
    - `tests/test_experiment_runner_retries.py` (integration with flaky experiment)
    - Additional tests to validate templating behavior across nested params.
- Suggested next steps
  - Add backoff logic (exponential) for retries and per-run timeout support.
- Executed follow-ups
  - Backoff not implemented (deferred); retries implemented with simple immediate retries.

Phase 6 — Experiment factory: build experiments from config dicts
- What I implemented
  - Extended `experiments/factory.py` with `build_experiment_from_config(key, config, X=None, global_config=None)` that merges top-level keys with `params` and constructs the experiment instance. This helps to centralise experiment instantiation from configs.
  - Added unit tests:
    - `tests/test_experiment_factory_from_config.py`
    - `tests/test_experiment_runner_build_from_config_integration.py`
- Suggested next steps
  - Update `ExperimentRunner` to optionally use `build_experiment_from_config` when a full experiment config object (not just params) is passed.
- Executed follow-ups
  - `ExperimentRunner` currently accepts configs as its `experiments` input and uses `params` to instantiate experiments; `build_experiment_from_config` is available for future use (left as next-step wiring).

Phase 7 — Backfill config examples and docs
- What I implemented
  - Created examples under `config/experiments/topic_example.yaml` and `config/pipelines/topic_pipeline_example.yaml`.
  - Added docs `docs/migration_phase7.md` with migration guidance and examples.
  - Added `pytest.ini` to avoid collecting top-level scripts during test runs.
  - Wrote `tests/test_phase7_pipeline_config_run.py` to assert the newly created example pipeline loads.
- Suggested next steps
  - Review example configs and update `PipelineFactory` to understand `ref` semantics and experiment refs in future phases.
- Executed follow-ups
  - Example files created and tests passed.

Phase 8 — Migration utility & directory-based migration
- What I implemented
  - Migration utility: `utils/migrate_pipelines.py` which:
    - Reads legacy `config/pipelines.yaml`.
    - Writes per-pipeline YAML to `config/migrated/pipelines/`.
    - Extracts inline experiments to `config/migrated/experiments/` and replaces them in the pipeline YAML with `ref` entries.
  - Test: `tests/test_migrate_pipelines.py` to run the migration and assert that files were written.
- Suggested next steps
  - Add a `--dry-run` mode to the migration script.
  - Implement directory-aware `PipelineFactory` that reads `config/pipelines/` and resolves `ref` entries.
  - Provide a one-off migration report and optional commit hook to move configs.
- Executed follow-ups
  - Migration script implemented and tested; dry-run and directory-aware loader are next steps.


Files created and modified (short description)

New files created (path -> brief description)
- `experiments/runner.py` — ExperimentRunner (sweeps, overrides, retries, MLflow lifecycle, evaluation & viz orchestration)
- `evaluators/runner.py` — EvaluationRunner (thin wrapper to call evaluators)
- `visualisations/runner.py` — VisualisationRunner (instantiation + plot invocation, returns artifact paths)
- `utils/migrate_pipelines.py` — Migration utility to translate legacy `config/pipelines.yaml` into layered files
- `docs/migration_phase7.md` — Phase 7 documentation and migration notes
- `docs/migration_report.md` — (this file) consolidated migration report (generated by this step)
- `config/experiments/topic_example.yaml` — example experiment config
- `config/pipelines/topic_pipeline_example.yaml` — example pipeline config
- `config/migrated/pipelines/` and `config/migrated/experiments/` — directories produced by migration script (populated by migration run)
- `pytest.ini` — test configuration to exclude `scripts/` from collection

Test files added (path -> brief description)
- `tests/test_experiment_runner_sweep.py` — sweep expansion unit test
- `tests/test_experiment_runner_dryrun.py` — dry-run test with dummy experiment
- `tests/text_experiment_runner_dryrun.py` — text experiment dry-run test
- `tests/test_experiment_runner_mlflow_integration.py` — MLflow lifecycle integration test
- `tests/test_topic_modeling_experiment_unit.py` — unit test for TopicModelingExperiment return contract
- `tests/test_experiment_runner_topic_integration.py` — integration test for TopicModelingExperiment via runner
- `tests/test_experiment_pipeline_unit.py` — pipeline delegation unit test
- `tests/test_experiment_pipeline_integration.py` — pipeline integration test (delegation)
- `tests/test_experiment_runner_eval_viz_unit.py` — unit test for eval/viz calls
- `tests/test_experiment_runner_eval_viz_integration.py` — integration test for eval/viz flow
- `tests/test_experiment_runner_sweep_overrides.py` — sweep+overrides unit test
- `tests/test_experiment_runner_retries.py` — retry behavior test
- `tests/test_experiment_runner_templating_nested.py` — nested templating unit test
- `tests/test_experiment_runner_sweep_eval_viz_integration.py` — end-to-end sweep+eval+viz integration
- `tests/test_experiment_factory_from_config.py` — factory build-experiment unit test
- `tests/test_experiment_runner_build_from_config_integration.py` — runner/experiment-from-config integration test
- `tests/test_phase7_pipeline_config_run.py` — Phase 7 example config loader test
- `tests/test_migrate_pipelines.py` — migration utility test

Files modified (path -> brief description)
- `experiments/base.py` — removed MLflow start/end from Experiment base, added `collect_params()` placeholder
- `experiments/topic_modeling_experiment.py` — removed inline MLflow calls and visualization/evaluation side-effects; returns structured result and `collect_params()` for logging
- `pipelines/experiment_pipeline.py` — updated to prepare experiment params and delegate to `ExperimentRunner.run_experiments()` instead of instantiating experiments inline
- `experiments/factory.py` — added `build_experiment_from_config()` to build experiment instances from config dicts

Test and quality status
- The focused tests added during this migration (unit + integration for each phase) were executed. The specific test files referenced above were run during development and passed in the environment used to make these changes.

How to run the migration script and a focused test set

- Run the migration script (writes migrated pipeline & experiment YAMLs):

```cmd
python utils\migrate_pipelines.py
```

- Run a focused set of tests (examples):

```cmd
python -m pytest tests\test_experiment_runner_sweep.py -q
python -m pytest tests\test_experiment_runner_eval_viz_unit.py -q
python -m pytest tests\test_experiment_runner_sweep_eval_viz_integration.py -q
```

- Run the full new test suite (may take longer):

```cmd
python -m pytest tests -q
```

Notes and caveats
- Visualisation API: I recommended and implemented a simple standard: viz.plot(df, model=None, **plot_kwargs). Some existing visualisations in the codebase may need minor updates to accept `model` as an optional kwarg or accept `**kwargs`.
- MLflow: MLflow lifecycle (start, log_param, log_metric, log_artifact, end_run) is centrally managed by `ExperimentRunner`. Experiments should not call MLflow.start_run or log directly.
- Migration: the `utils/migrate_pipelines.py` script is intentionally conservative: it writes files into `config/migrated/` and does not delete or alter the original `config/pipelines.yaml`.

Next steps (high-level, cross-phase)
- (Short-term)
  - Implement a `--dry-run` mode for `utils/migrate_pipelines.py` that prints intended outputs without writing files (not yet executed).
  - Implement a directory-aware loader in `PipelineFactory.build_pipelines_from_yaml()` that accepts a directory path or resolves `ref` entries (not yet executed).
  - Update any visualisations that do not follow the standard plot signature (some may require adding `**kwargs` or `model=None`) (partially executed by advising; not executed across all visualisations).

- (Medium-term)
  - Migrate remaining experiments to return the standardized run result dict and to implement `collect_params()`.
  - Improve retry logic with configurable exponential backoff (implement in ExperimentRunner).
  - Add more end-to-end tests that exercise the full stack on a very small dataset (smoke tests).

- (Long-term)
  - Add CI job(s) to run the new tests and validate migration steps automatically on PRs.
  - Consider adding a small JSON Schema or other validation for the layered config files (pre-flight checks before running migrations). This was intentionally deferred to keep the migration fast and low-risk.

Appendix — Quick file map (full paths)

Created files
- c:\Users\Sinead\Gavin\political_discourse\experiments\runner.py
- c:\Users\Sinead\Gavin\political_discourse\evaluators\runner.py
- c:\Users\Sinead\Gavin\political_discourse\visualisations\runner.py
- c:\Users\Sinead\Gavin\political_discourse\utils\migrate_pipelines.py
- c:\Users\Sinead\Gavin\political_discourse\docs\migration_phase7.md
- c:\Users\Sinead\Gavin\political_discourse\config\experiments\topic_example.yaml
- c:\Users\Sinead\Gavin\political_discourse\config\pipelines\topic_pipeline_example.yaml
- c:\Users\Sinead\Gavin\political_discourse\pytest.ini
- Many tests under `c:\Users\Sinead\Gavin\political_discourse\tests\` (listed earlier)

Modified files
- c:\Users\Sinead\Gavin\political_discourse\experiments\base.py
- c:\Users\Sinead\Gavin\political_discourse\experiments\topic_modeling_experiment.py
- c:\Users\Sinead\Gavin\political_discourse\pipelines\experiment_pipeline.py
- c:\Users\Sinead\Gavin\political_discourse\experiments\factory.py

If you want I will:
- Add `--dry-run` to `utils/migrate_pipelines.py` and implement directory resolving inside `PipelineFactory` next.
- Run the entire test suite and produce a short CI job (GitHub Actions) scaffold.

If you'd like a shorter summary / changelog suitable for a PR description, I can produce that next.

