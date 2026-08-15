# experiments/runner.py
from typing import List, Dict, Any
import itertools
import mlflow
import copy
from logs.logger import get_logger
from experiments.factory import ExperimentFactory
from evaluators.runner import EvaluationRunner
from visualisations.runner import VisualisationRunner
from config.config_loader import ConfigLoader
import psutil
import os
import hashlib
import json
import pandas as pd

logger = get_logger("ExperimentRunner")


class ExperimentRunner:
    """Central runner responsible for expanding sweeps, applying overrides,
    iterating concrete runs and managing MLflow lifecycle for each run.

    Minimal, backward-compatible behaviour for Phase 1 of migration.
    """

    def __init__(self, mlflow_enabled: bool = True, config_loader: ConfigLoader = None):
        self.mlflow_enabled = mlflow_enabled
        self.logger = logger
        # helper runners
        self.eval_runner = EvaluationRunner()
        self.viz_runner = VisualisationRunner()
        # Config loader for resolving refs (can be injected for tests)
        self.config_loader = config_loader or ConfigLoader()
        self.logger.info(f"ExperimentRunner initialized with MLflow enabled: {self.mlflow_enabled}")

    @staticmethod
    def _flatten_dict(d: Dict[str, Any], parent_key: str = "", sep: str = "_") -> Dict[str, Any]:
        """Flatten nested dict for logging purposes. e.g. {'a': {'b': 1}} -> {'a_b': 1}"""
        items = {}
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(ExperimentRunner._flatten_dict(v, new_key, sep=sep))
            else:
                items[new_key] = v
        return items

    def _log_cuda_memory(self, label):
        try:
            import torch

            if torch.cuda.is_available():
                allocated = torch.cuda.memory_allocated() / 1024 ** 3
                reserved = torch.cuda.memory_reserved() / 1024 ** 3

                self.logger.info(
                    f"[CUDA] {label}: "
                    f"allocated={allocated:.2f} GB, "
                    f"reserved={reserved:.2f} GB"
                )
        except Exception:
            pass

    def expand_sweeps(self, experiments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Expand sweep specs into a list of concrete experiment configs.

        Sweeps are specified with a 'sweep' key mapping dotted paths to lists.
        Example:
          sweep:
            params.model_params.nr_topics: [5,10]
            params.embedding_model.name: ['a','b']

        This implements a simple cartesian product across listed values.
        """
        self.logger.info(f"Expanding sweeps for {len(experiments)} experiments")
        concrete = []

        for exp in experiments:
            sweep = exp.get("sweep")
            process = psutil.Process(os.getpid())
            self.logger.info(f"Current memory usage before expanding sweep: {process.memory_info().rss / (1024 **3):.2f} GB")
            self.logger.info(f"Processing experiment with sweep: {sweep}")

            if not sweep:
                concrete.append(exp)
                continue

            # Parse sweep keys and values
            sweep_items = list(sweep.items())
            keys = [k for k, _ in sweep_items]
            values_lists = [v if isinstance(v, list) else [v] for _, v in sweep_items]

            self.logger.info(f"Sweep keys: {keys}")
            self.logger.info(f"Values: {values_lists}")

            # PREPROCESS: detect any sweep value that is a list of model configs (dicts)
            # and expand nested parameter lists inside those model configs into concrete variants.
            processed_values_lists = []
            for key, vlist in zip(keys, values_lists):
                if isinstance(vlist, list) and len(vlist) > 0 and all(isinstance(el, dict) for el in vlist):
                    # Attempt to expand nested model configs
                    try:
                        expanded = self._expand_nested_model_configs(vlist)
                        self.logger.info(
                            "Expanded sweep key '%s' from %d model configs into %d concrete variants",
                            key,
                            len(vlist),
                            len(expanded),
                        )
                        self.logger.debug("Expanded models for %s: %s", key, expanded)
                        processed_values_lists.append(expanded)
                    except Exception as e:
                        self.logger.warning(f"Could not expand nested model configs for key {key}: {e}")
                        processed_values_lists.append(vlist)
                else:
                    processed_values_lists.append(vlist)

            # Now perform cartesian product across processed_values_lists
            for combo in itertools.product(*processed_values_lists):
                self.logger.info(f"Memory usage before creating new experiment config for combo {combo}: {process.memory_info().rss / (1024 **3):.2f} GB")
                # Deep copy base exp to avoid shared nested dict references across combos
                new_exp = copy.deepcopy(exp)
                if "sweep" in new_exp:
                    del new_exp["sweep"]
                # Apply each sweep selection into nested dict path
                for key_path, val in zip(keys, combo):
                    self._set_by_path(new_exp, key_path, val)
                concrete.append(new_exp)

        self.logger.info("Total concrete runs generated: %d", len(concrete))
        return concrete

    @staticmethod
    def _set_by_path(d: Dict[str, Any], path: str, value: Any):
        """Set a nested dict value given a dotted path, creating intermediate dicts as needed.
        Path may use '.' or '/' as separators. E.g. 'params.model_params.nr_topics'."""
        sep = "." if "." in path else "/"
        parts = path.split(sep)
        cur = d
        for p in parts[:-1]:
            if p not in cur or not isinstance(cur[p], dict):
                cur[p] = {}
            cur = cur[p]
        cur[parts[-1]] = value

    def _merge_configs(self, base: Dict[str, Any], *overlays: Dict[str, Any]) -> Dict[str, Any]:
        """Merge multiple config dicts into a new dict. Later overlays override earlier keys.

        - For the top-level 'params' key perform a deep merge (dict update) so that
          experiment params override model/evaluator params but preserve unspecified keys.
        - Does not mutate inputs.
        """
        out = copy.deepcopy(base) if base is not None else {}
        for ov in overlays:
            if not ov:
                continue
            for k, v in ov.items():
                if k == "params":
                    out_params = out.get("params", {}) or {}
                    # shallow merge of params dicts
                    merged = copy.deepcopy(out_params)
                    if isinstance(v, dict):
                        merged.update(v)
                    out["params"] = merged
                else:
                    # overlay simple replacement
                    out[k] = copy.deepcopy(v)
        return out

    def _resolve_and_build_run_config(self, exp_cfg: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve refs inside exp_cfg and merge referenced configs into a single run config.

        Supports keys: model_ref, evaluator_ref, visualisation_ref, preprocessing_ref
        or alternatively model, evaluator, visualisation, preprocessing. Later keys override.
        Returns a NEW dict (does not mutate input).
        """
        # If the exp_cfg is a dict with a top-level 'ref' plus other keys (e.g., sweep),
        # load the referenced file and then overlay other keys on top so that sweep/overrides
        # remain attached to the run config.
        try:
            if isinstance(exp_cfg, dict) and "ref" in exp_cfg and (len(exp_cfg.keys()) > 1):
                # load referenced content
                ref_val = exp_cfg.get("ref")
                if isinstance(ref_val, str):
                    # Resolve path relative to cwd
                    try:
                        base_loaded = self.config_loader.load_file(ref_val)
                    except Exception:
                        # try resolving against working dir
                        base_loaded = self.config_loader.load_file(ref_val)
                else:
                    base_loaded = {}
                # Merge loaded base with exp_cfg where exp_cfg keys override loaded base
                merged_input = copy.deepcopy(base_loaded)
                # copy other keys from exp_cfg (except 'ref') into merged_input
                for k, v in exp_cfg.items():
                    if k == "ref":
                        continue
                    merged_input[k] = v
                resolved = self.config_loader.resolve_refs(merged_input)
            else:
                resolved = self.config_loader.resolve_refs(exp_cfg)
        except Exception as e:
            self.logger.warning(f"Could not resolve refs for experiment config: {e}")
            resolved = copy.deepcopy(exp_cfg)

        # Collect referenced configs (after resolution they should be dicts)
        model_conf = resolved.get("model_ref") or resolved.get("model") or {}
        eval_conf = resolved.get("evaluator_ref") or resolved.get("evaluator") or {}
        viz_conf = resolved.get("visualisation_ref") or resolved.get("visualisation") or {}
        prep_conf = resolved.get("preprocessing_ref") or resolved.get("preprocessing") or {}
        sweep = resolved.get("sweep")

        # Merge in order: model <- evaluator <- visualisation <- preprocessing <- experiment
        merged = self._merge_configs(model_conf or {}, eval_conf or {}, viz_conf or {}, prep_conf or {}, resolved or {})

        if sweep is not None:
            merged["sweep"] = sweep
        return merged

    def run_experiments(self, experiment_type: str, experiments: List[Dict[str, Any]], global_config: Dict[str, Any] = None, X=None) -> List[Dict[str, Any]]:
        """Run a list of experiment configs for a given experiment_type (key used by ExperimentFactory).

        Each concrete experiment config is expanded from potential sweep specs. For each run:
         - create a run_name
         - start an MLflow run (if enabled)
         - instantiate the experiment via ExperimentFactory.get_experiment(experiment_type, **params)
         - call experiment.run(X=X)
         - collect results and MLflow-log flat params

        Returns a list of run results (dicts returned by experiments).
        """
        self.logger.info(f"Running {len(experiments)} experiments of type '{experiment_type}' with global config: {global_config}")
        results = []
        global_config = global_config or {}

        # First, resolve refs and merge referenced configs for each experiment
        prepared = []
        for exp in experiments:
            sweep = exp.get("sweep")
            self.logger.info(f"Preparing experiment config with sweep: {sweep}")
            if not isinstance(exp, dict):
                prepared.append(exp)
                continue
            try:
                run_cfg = self._resolve_and_build_run_config(exp)
            except Exception:
                run_cfg = copy.deepcopy(exp)

            if sweep is not None:
                run_cfg["sweep"] = sweep
            prepared.append(run_cfg)

        self.logger.info(f"Prepared experiment config keys: {prepared[0].keys()}")
        self.logger.info(f"Prepared {len(prepared)} experiments after resolving refs and merging configs")
        concrete_exps = self.expand_sweeps(prepared)
        self.logger.info(f"Expanded {len(experiments)} experiments into {len(concrete_exps)} concrete runs")

        # Apply overrides (if any)
        concrete_exps = self._apply_overrides(concrete_exps)

        # Calculate hashes BEFORE any experiments are executed.
        # Hashes are kept separately from the experiment configurations.
        experiment_hashes = []

        for exp_cfg in concrete_exps:
            experiment_hash = self._generate_config_hash(exp_cfg)
            experiment_hashes.append(experiment_hash)

            self.logger.info(
                f"Generated experiment hash: {experiment_hash}"
            )

        # Respect optional max_runs at experiment-level or global
        max_runs_global = global_config.get("max_runs") if isinstance(global_config, dict) else None
        if max_runs_global is not None:
            concrete_exps = concrete_exps[:max_runs_global]
            experiment_hashes = experiment_hashes[:max_runs_global]

        for idx, (exp_cfg, experiment_hash) in enumerate(
                zip(concrete_exps, experiment_hashes),
                start=1
        ):
            params = exp_cfg.get("params", {})

            max_runs = exp_cfg.get("max_runs")
            if max_runs is not None and idx > max_runs:
                break

            base_run_name = (
                    exp_cfg.get("run_name")
                    or f"{experiment_type}_run{idx}"
            )

            run_name = self._format_run_name(
                base_run_name,
                params,
                experiment_hash
            )

            save_path = exp_cfg.get("save_path")
            raw_save_path = exp_cfg.get("save_path")

            save_path = (
                self._format_run_name(
                    raw_save_path,
                    params,
                    experiment_hash
                )
                if raw_save_path
                else None
            )
            self.logger.info(f"Starting run '{run_name}' with save_path: {save_path}")

            # push formatted value back into config so downstream sees it
            if save_path:
                # exp_cfg.setdefault("params", {})
                # exp_cfg["params"]["save_path"] = save_path
                exp_cfg["save_path"] = save_path
                params["save_path"] = save_path

            # retries support (default 0)
            retries = exp_cfg.get("retries", params.get("retries", 0) if isinstance(params, dict) else 0)

            if self.mlflow_enabled:
                mlflow_experiment = (global_config.get("mlflow_experiment")
                                        if global_config
                                        else None
                                    ) or exp_cfg.get("mlflow_experiment")

                self.logger.info(
                    f"Setting MLflow experiment to "
                    f"'{mlflow_experiment}' for run '{run_name}'"
                )

                mlflow.set_experiment(mlflow_experiment)

                if self._run_exists(run_name):
                    self.logger.info(
                        f"Skipping existing completed run '{run_name}'"
                    )
                    continue

            # Begin MLflow run
            try:
                if self.mlflow_enabled:
                    #self.logger.info(f"Setting MLflow experiment to '{global_config.get('mlflow_experiment') or exp_cfg.get('mlflow_experiment')}' for run '{run_name}'")
                    #mlflow.set_experiment(global_config.get("mlflow_experiment") if global_config else exp_cfg.get("mlflow_experiment") )
                    mlflow.start_run(run_name=run_name)
                    self.logger.info(f"Started MLflow run: {run_name}")

                # Instantiate experiment via factory. Prefer building from full config if available
                try:
                    # If exp_cfg looks like a full config (contains more than just params) use build_experiment_from_config
                    is_full_config = any(k in exp_cfg for k in ("model_ref", "model", "evaluator_ref", "evaluator", "visualisation_ref", "visualisation", "preprocessing_ref", "preprocessing")) or ("params" in exp_cfg and len(exp_cfg.keys()) > 1)
                    self._log_cuda_memory("BEFORE experiment")
                    if is_full_config:
                        self.logger.info(f"Building experiment from full config for '{experiment_type}'")
                        # Ensure run_name is visible to builder via top-level key
                        cfg_for_build = copy.deepcopy(exp_cfg)
                        if "run_name" not in cfg_for_build:
                            cfg_for_build["run_name"] = run_name
                        exp_instance = ExperimentFactory.build_experiment_from_config(experiment_type, cfg_for_build, X=X, global_config=global_config)
                    else:
                        self.logger.info(f"Instantiating experiment '{experiment_type}' with params keys: {list(params.keys())}")
                        # Build kwargs for instantiation; avoid passing X twice
                        kwargs = dict(params) if isinstance(params, dict) else {}
                        kwargs["name"] = run_name
                        if X is not None and "X" not in kwargs:
                            kwargs["X"] = X
                        exp_instance = ExperimentFactory.get_experiment(experiment_type, **kwargs)
                except Exception as e:
                    self.logger.error(f"Could not instantiate experiment '{experiment_type}': {e}")
                    raise

                # Run the experiment with retry logic
                self.logger.info(f"Running experiment '{run_name}' (retries={retries})")
                attempt = 0
                last_exc = None
                run_result = None
                attempts_allowed = (retries or 0) + 1
                while attempt < attempts_allowed:
                    try:
                        run_result = exp_instance.run()
                        self._log_cuda_memory("AFTER experiment.run()")
                        last_exc = None
                        break
                    except Exception as e:
                        last_exc = e
                        attempt += 1
                        self.logger.warning(f"Experiment '{run_name}' attempt {attempt} failed: {e}")
                        if attempt >= attempts_allowed:
                            self.logger.error(f"Experiment '{run_name}' failed after {attempts_allowed} attempts")
                            raise
                        else:
                            self.logger.info(f"Retrying experiment '{run_name}' (attempt {attempt+1}/{attempts_allowed})")

                # Log flattened params to MLflow (including experiment.collect_params())
                if self.mlflow_enabled:
                    flat = self._flatten_dict(params)
                    try:
                        exp_params = exp_instance.collect_params() if hasattr(exp_instance, "collect_params") else {}
                        flat.update(self._flatten_dict(exp_params))
                    except Exception:
                        pass

                    for k, v in flat.items():
                        try:
                            self.logger.info(f"Logging param to MLflow for run '{run_name}': {k}={v}")
                            mlflow.log_param(k, v)
                        except Exception:
                            # skip non-serializable params
                            self.logger.debug(f"Skipping logging param {k} as it is not serializable: {v}")

                # Evaluation
                try:
                    evaluator_cfg = params.get("evaluator_name") or run_result.get("metadata", {}).get("evaluator_name")
                    if evaluator_cfg:
                        self.logger.info(f"Running evaluator for run '{run_name}' using config: {evaluator_cfg}")

                        eval_out = self.eval_runner.evaluate(run_result, evaluator_cfg if isinstance(evaluator_cfg, dict) else {"name": evaluator_cfg})

                        evaluator_params = (
                                params.get("evaluator_params")
                                or run_result.get("metadata", {}).get("evaluator_params")
                                or {}
                        )
                        self.logger.info(f"Evaluator params for run '{run_name}': {evaluator_params}")
                        flat.update(self._flatten_dict(evaluator_params))

                        metrics = eval_out.get("metrics", {})
                        artifacts = eval_out.get("artifacts", [])



                        # Log metrics/artifacts to MLflow
                        if self.mlflow_enabled:
                            for mk, mv in (metrics or {}).items():
                                try:
                                    if isinstance(mv, (int, float)):
                                        mlflow.log_metric(mk, mv)
                                except Exception:
                                    self.logger.debug(f"Could not log metric {mk}: {mv}")
                            for art in artifacts:
                                try:
                                    mlflow.log_artifact(art)
                                except Exception:
                                    self.logger.debug(f"Could not log artifact {art}")
                    else:
                        metrics = {}
                        artifacts = []
                except Exception as e:
                    self.logger.warning(f"Evaluator failed for run '{run_name}': {e}")
                    metrics = {}
                    artifacts = []

                # Visualisations
                try:
                    viz_cfgs = params.get("visualisations") or run_result.get("metadata", {}).get("visualisations") or []
                    self.logger.info(f"Visualisation configs for run '{run_name}': {viz_cfgs}")
                    # run_save_path = (
                    #         exp_cfg.get("params", {}).get("save_path")
                    #         or exp_cfg.get("save_path")
                    # )
                    run_save_path = exp_cfg.get("save_path")
                    self.logger.info(f"Save path for Visualisations in run '{run_name}': {run_save_path}")
                    viz_artifacts = []

                    for viz_cfg in viz_cfgs:
                        self.logger.info(f"Rendering visualisation for run '{run_name}': {viz_cfg}")
                        init = viz_cfg.setdefault("init", {})

                        if "output_dir" not in init and run_save_path:
                            init["output_dir"] = run_save_path

                        model = getattr(exp_instance, "model", None)
                        produced = self.viz_runner.render(run_result, viz_cfg, model=model)
                        viz_artifacts.extend(produced or [])



                    # log viz artifacts
                    if self.mlflow_enabled:
                        for art in viz_artifacts:
                            try:
                                mlflow.log_artifact(art)
                            except Exception:
                                self.logger.debug(f"Could not log viz artifact {art}")
                except Exception as e:
                    self.logger.warning(f"Visualisation failed for run '{run_name}': {e}")

                # Also log any artifacts returned by the experiment itself
                try:
                    for art in (run_result.get("artifacts") or []):
                        if self.mlflow_enabled:
                            try:
                                mlflow.log_artifact(art)
                            except Exception:
                                self.logger.debug(f"Could not log experiment artifact {art}")
                except Exception:
                    pass

                # Model is no longer required after evaluation and visualisations
                metadata = run_result.get("metadata")


                if isinstance(run_result.get("metadata"), dict):
                    run_result["metadata"].pop("model", None)

                if exp_instance is not None:
                    exp_instance.model = None

                self.logger.info(
                    f"Model in run_result metadata: "
                    f"{'model' in run_result.get('metadata', {})}"
                )

                results.append({"run_name": run_name, "result": run_result, "metrics": metrics, "artifacts": artifacts})



            except Exception as e:
                self.logger.error(f"Run '{run_name}' failed: {e}")
                results.append({"run_name": run_name, "error": str(e)})
            finally:
                if self.mlflow_enabled:
                    try:
                        mlflow.end_run()
                        self.logger.info(f"Ended MLflow run: {run_name}")
                    except Exception:
                        pass

                self._log_cuda_memory("BEFORE CLEANUP")

                import gc
                import types

                try:

                    # NOW remove the remaining local references
                    if 'run_result' in locals():
                        del run_result

                    if 'exp_instance' in locals():
                        del exp_instance

                    if 'model' in locals():
                        del model


                except Exception as e:
                    self.logger.exception(
                        f"Cleanup failed: {e}"
                    )

                gc.collect()

                try:
                    import torch

                    if torch.cuda.is_available():
                        torch.cuda.empty_cache()
                        torch.cuda.ipc_collect()

                except Exception as cleanup_error:
                    self.logger.warning(
                        f"CUDA cleanup failed: {cleanup_error}"
                    )

                gc.collect()

                self._log_cuda_memory("AFTER cleanup")

        return results

    def _apply_overrides(self, concrete: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """If an experiment config contains an 'overrides' key (list of dicts),
        apply each override to each concrete config and return expanded list.
        Overrides are dotted-path dicts (same path format as sweep keys).
        """
        out = []
        for exp in concrete:
            overrides = exp.get("overrides")
            if not overrides:
                out.append(exp)
                continue

            for ov in overrides:
                new_exp = copy.deepcopy(exp)
                # remove overrides from new_exp
                if "overrides" in new_exp:
                    del new_exp["overrides"]
                # apply each override path
                for path, val in ov.items():
                    self._set_by_path(new_exp, path, val)
                out.append(new_exp)

        return out

    def _format_run_name(
            self,
            template: str,
            params: Dict[str, Any],
            experiment_hash: str
    ) -> str:
        """Format run name using the precomputed experiment hash."""
        try:
            flat = self._flatten_dict(params)

            base_name = template.format_map(
                {
                    k: str(v)
                    for k, v in flat.items()
                }
            )
        except Exception:
            base_name = template

        return f"{base_name}_{experiment_hash}"

    def _generate_config_hash(
            self,
            exp_cfg: Dict[str, Any],
            length: int = 8
    ) -> str:
        """
        Generate a deterministic hash from the experiment configuration,
        excluding the input DataFrame.
        """
        hash_config = copy.deepcopy(exp_cfg)

        params = hash_config.get("params")
        self.logger.info(
            f"Hashing experiment configuration (excluding params.X)"
        )
        if isinstance(params, dict):
            params.pop("X", None)

        canonical = json.dumps(
            hash_config,
            sort_keys=True,
            separators=(",", ":"),
            default=str
        )

        self.logger.info(
            f"Generated hash for experiment configuration: {canonical}"
        )

        return hashlib.sha256(
            canonical.encode("utf-8")
        ).hexdigest()[:length]

    def _run_exists(self, run_name: str) -> bool:
        """
        Check whether an MLflow run with this run_name already completed.
        """

        self.logger.info(
            f"Checking whether MLflow run already exists: '{run_name}'"
        )

        runs = mlflow.search_runs(
            filter_string=f"tags.mlflow.runName = '{run_name}'",
            run_view_type=mlflow.entities.ViewType.ACTIVE_ONLY
        )

        if runs.empty:
            self.logger.info(
                f"No existing MLflow run found for '{run_name}' - "
                f"will execute"
            )
            return False

        statuses = runs["status"].tolist()

        if "RUNNING" in statuses:
            self.logger.warning(
                f"Found orphaned RUNNING MLflow run(s) for '{run_name}'"
            )
            return False

        self.logger.info(
            f"Found existing MLflow run(s) for '{run_name}' "
            f"with status: {statuses}"
        )

        completed = runs[
            runs["status"] == "FINISHED"
            ]

        if not completed.empty:
            self.logger.info(
                f"Skipping run '{run_name}' because a FINISHED "
                f"MLflow run already exists"
            )
            return True

        self.logger.info(
            f"Existing run(s) found for '{run_name}', "
            f"but none completed successfully - will execute"
        )

        return False

    def _expand_nested_model_configs(self, model_configs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Expand a list of model configs by replacing nested list parameters with cartesian products.

        For example, given a model config like:
          {"type": "my_model", "params": {"layers": [64, 128], "activation": "relu"}}
        which specifies a list of values for the 'layers' param, this would produce:
          - {"type": "my_model", "params": {"layers": 64, "activation": "relu"}}
          - {"type": "my_model", "params": {"layers": 128, "activation": "relu"}}

        Assumes all nested lists in a model config should be expanded this way.
        """
        self.logger.info(f"Expanding {len(model_configs)} nested model configs")
        expanded_configs = []
        for model_cfg in model_configs:
            # Collect all param paths that have list values
            list_paths = []

            def _find_list_paths(d: Dict[str, Any], base_path: str = ""):
                for k, v in d.items():
                    path = f"{base_path}{'.' if base_path else ''}{k}"
                    if isinstance(v, dict):
                        _find_list_paths(v, path)
                    elif isinstance(v, list):
                        list_paths.append(path)

            _find_list_paths(model_cfg)

            # Generate cartesian product of values across all list paths
            all_combos = []
            for path in list_paths:
                # Resolve to actual list value in model_cfg
                cur = model_cfg
                for part in path.split("."):
                    if isinstance(cur, dict):
                        cur = cur.get(part)
                    else:
                        cur = None
                if isinstance(cur, list):
                    all_combos.append(cur)

            # If there are no list-valued params, or just one, we can only clone the config
            if len(all_combos) == 0:
                expanded_configs.append(copy.deepcopy(model_cfg))
            elif len(all_combos) == 1:
                for val in all_combos[0]:
                    # Simple case, just clone and replace the single list value
                    new_cfg = copy.deepcopy(model_cfg)
                    for path in list_paths:
                        self._set_by_path(new_cfg, path, val)
                    expanded_configs.append(new_cfg)
            else:
                # Complex case: multiple list-valued params, need cartesian product
                for combo in itertools.product(*all_combos):
                    new_cfg = copy.deepcopy(model_cfg)
                    for path, val in zip(list_paths, combo):
                        self._set_by_path(new_cfg, path, val)
                    expanded_configs.append(new_cfg)

        self.logger.info(f"Expanded into {len(expanded_configs)} total model configs")
        return expanded_configs
