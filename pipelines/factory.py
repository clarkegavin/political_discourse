# pipelines/factory.py
from typing import List, Dict, Any, Type
from logs.logger import get_logger
import importlib
from pipelines.base import Pipeline
from config.config_loader import ConfigLoader
from pathlib import Path
import copy

logger = get_logger("PipelineFactory")


class PipelineFactory:
    """Factory for creating and registering pipeline instances.

    - build_pipelines_from_path(path): Accepts a file or directory. If a file is provided,
      it loads the file (supports legacy single `pipelines` key). If a directory is
      provided, it loads all .yaml files and treats each as a pipeline config source.
    - Uses ConfigLoader to load and resolve `ref` entries and to load experiment refs.
    - Maintains a registry of created pipelines by name.
    """

    _registry: Dict[str, Pipeline] = {}

    @classmethod
    def register_pipeline(cls, name: str, pipeline: Pipeline):
        if not name:
            logger.warning("Attempted to register pipeline with empty name")
            return
        cls._registry[name] = pipeline
        logger.info(f"Registered pipeline: {name}")

    @classmethod
    def get_pipeline(cls, name: str) -> Pipeline:
        pipeline = cls._registry.get(name)
        if not pipeline:
            logger.warning(f"Pipeline '{name}' not found in registry")
        return pipeline

    @classmethod
    def build_pipelines_from_path(cls, path: str) -> List[Pipeline]:
        """Build pipelines from a file or directory path.

        - If `path` is a file: load single pipeline config (supports legacy `pipelines` list).
        - If `path` is a directory: load all `.yaml` files and treat each as a pipeline file.
        - Resolves refs using ConfigLoader and inlines experiment refs into `params.experiments`.
        """
        cfg_loader = ConfigLoader()
        p = Path(path)

        pipeline_entries: List[Dict[str, Any]] = []

        if p.is_dir():
            logger.info("Loading pipeline configs from directory: %s", str(p))
            loaded = cfg_loader.load_dir(str(p))  # returns dict keyed by filename
            for key, content in loaded.items():
                if isinstance(content, dict) and "pipelines" in content:
                    pipeline_entries.extend(content.get("pipelines", []))
                else:
                    pipeline_entries.append(content)
        elif p.is_file():
            logger.info("Loading pipeline config from file: %s", str(p))
            loaded = cfg_loader.load_file(str(p))
            if isinstance(loaded, dict) and "pipelines" in loaded:
                pipeline_entries.extend(loaded.get("pipelines", []))
            else:
                pipeline_entries.append(loaded)
        else:
            raise FileNotFoundError(f"Path not found: {path}")

        created: List[Pipeline] = []
        base_dir = p.parent if p.is_file() else p

        for entry in pipeline_entries:
            # Resolve refs within pipeline entry
            try:
                resolved_entry = cfg_loader.resolve_refs(copy.deepcopy(entry))
            except Exception as e:
                logger.error(f"Failed resolving refs for pipeline '{entry.get('name')}': {e}")
                resolved_entry = copy.deepcopy(entry)

            # Resolve experiment_refs (if any) and inline as params.experiments
            experiments_resolved: List[Dict[str, Any]] = []
            if "experiment_refs" in resolved_entry:
                refs = resolved_entry.get("experiment_refs") or []
                for ref in refs:
                    ref_path = Path(ref)
                    if not ref_path.is_absolute():
                        ref_path = (base_dir / ref_path).resolve()
                    try:
                        exp_loaded = cfg_loader.load_file(str(ref_path))
                        exp_resolved = cfg_loader.resolve_refs(copy.deepcopy(exp_loaded))
                        if isinstance(exp_resolved, dict) and "experiments" in exp_resolved:
                            experiments_resolved.extend(exp_resolved["experiments"])
                        elif isinstance(exp_resolved, list):
                            experiments_resolved.extend(exp_resolved)
                        else:
                            experiments_resolved.append(exp_resolved)
                    except Exception as e:
                        logger.error(f"Failed to load experiment ref {ref}: {e}")
            else:
                inline = resolved_entry.get("experiments") or (resolved_entry.get("params") or {}).get("experiments")
                if inline:
                    experiments_resolved = copy.deepcopy(inline)

            # Inject resolved experiments into params
            params: Dict[str, Any] = resolved_entry.get("params", {}) or {}
            if experiments_resolved:
                params["experiments"] = experiments_resolved
            resolved_entry["params"] = params

            pipeline_type = resolved_entry.get("type")
            if not pipeline_type:
                logger.warning(f"Pipeline type missing for '{resolved_entry.get('name')}', skipping")
                continue

            try:
                module_name, class_name = pipeline_type.rsplit(".", 1)
                module = importlib.import_module(module_name)
                klass: Type[Pipeline] = getattr(module, class_name)

                # Prefer from_config when available
                if hasattr(klass, "from_config") and callable(getattr(klass, "from_config")):
                    instance_kwargs = {}
                    # Some from_config implementations accept global_config; pass source path
                    try:
                        import inspect
                        sig = inspect.signature(klass.from_config)
                        if "global_config" in sig.parameters:
                            instance_kwargs["global_config"] = {"source_path": str(path)}
                    except Exception:
                        pass

                    pipeline_instance = klass.from_config(resolved_entry, **instance_kwargs)
                    logger.info(f"Used from_config() to create pipeline '{resolved_entry.get('name')}'")
                else:
                    params: Dict[str, Any] = resolved_entry.get("params", {})
                    # Special-case ExperimentPipeline to provide global_config historically
                    if klass.__name__ == "ExperimentPipeline":
                        pipeline_instance = klass(**params, global_config={"source_path": str(path)})
                        logger.info(f"Used __init__() to create ExperimentPipeline '{resolved_entry.get('name')}'")
                    else:
                        pipeline_instance = klass(**params)
                        logger.info(f"Used __init__() to create pipeline '{resolved_entry.get('name')}'")

                created.append(pipeline_instance)
                cls.register_pipeline(resolved_entry.get("name"), pipeline_instance)
                logger.info(f"Pipeline '{resolved_entry.get('name')}' ({pipeline_type}) created successfully")

            except Exception as e:
                logger.error(f"Failed to create pipeline '{resolved_entry.get('name')}': {e}")

        return created

    @classmethod
    def build_pipelines_from_yaml(cls, yaml_path: str) -> List[Pipeline]:
        """Backward-compatible wrapper for callers that still invoke the old API."""
        return cls.build_pipelines_from_path(yaml_path)
