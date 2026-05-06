import os
import logging
import yaml
import copy
from typing import Dict, Any, Set, Optional

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class ConfigLoader:
    """
    Simple layered YAML config loader with ref resolution.

    API:
      - load_file(path: str) -> dict
      - load_dir(path: str) -> dict
      - resolve_refs(config: dict) -> dict

    Notes:
      - Directory loads only files with `.yaml` extension and returns a dict keyed by filename (no extension).
      - `ref` nodes of the form `{'ref': 'relative/or/absolute/path.yaml'}` are replaced with the referenced file's contents.
      - Relative ref paths are resolved against the last-loaded file's directory (or the directory passed to `load_dir`).
      - The loader never modifies input structures in-place; results are deep copies.
    """

    def __init__(self, logger_obj: Optional[logging.Logger] = None) -> None:
        self._cache: Dict[str, Any] = {}
        self._working_dir: str = os.getcwd()
        self._last_loaded_file: Optional[str] = None
        if logger_obj is not None:
            global logger
            logger = logger_obj

    def load_file(self, path: str) -> Dict[str, Any]:
        abs_path = os.path.abspath(path)
        logger.info("Loading YAML file: %s", abs_path)
        content = self._load_file_no_sideeffects(abs_path)
        # Update working context after successful load
        self._last_loaded_file = abs_path
        self._working_dir = os.path.dirname(abs_path) or self._working_dir
        return copy.deepcopy(content) if content is not None else {}

    def load_dir(self, path: str) -> Dict[str, Dict[str, Any]]:
        abs_dir = os.path.abspath(path)
        logger.info("Loading YAML directory: %s", abs_dir)
        result: Dict[str, Dict[str, Any]] = {}
        if not os.path.isdir(abs_dir):
            raise NotADirectoryError(f"Not a directory: {abs_dir}")
        for fname in sorted(os.listdir(abs_dir)):
            if not fname.endswith(".yaml"):
                continue
            key = os.path.splitext(fname)[0]
            fpath = os.path.join(abs_dir, fname)
            content = self._load_file_no_sideeffects(os.path.abspath(fpath))
            result[key] = copy.deepcopy(content) if content is not None else {}
        # Update working context to this directory
        self._working_dir = abs_dir
        self._last_loaded_file = None
        return result

    def resolve_refs(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve refs inside `config`. Uses the loader's current working directory
        (set by the last `load_file` or `load_dir`) to resolve relative paths.
        Does not mutate the provided `config`.
        """
        base_dir = self._working_dir or os.getcwd()
        logger.info("Resolving refs with base directory: %s", base_dir)
        working_copy = copy.deepcopy(config)
        resolved = self._resolve_node(working_copy, base_dir, seen_paths=set())
        return resolved

    # --- internal helpers ---

    def _load_file_no_sideeffects(self, abs_path: str) -> Any:
        """
        Load YAML from abs_path, cache result. Does NOT modify self._working_dir or _last_loaded_file.
        """
        if abs_path in self._cache:
            logger.info("Using cached YAML file: %s", abs_path)
            return self._cache[abs_path]

        if not os.path.exists(abs_path):
            logger.error("YAML file not found: %s", abs_path)
            raise FileNotFoundError(f"Config file not found: {abs_path}")

        with open(abs_path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        self._cache[abs_path] = data
        logger.info("Loaded YAML file: %s", abs_path)
        return data

    def _resolve_node(self, node: Any, base_dir: str, seen_paths: Set[str]) -> Any:
        """
        Recursively resolve refs within the node. Returns a new node (doesn't mutate input).
        """
        # Dict: check for direct ref node
        if isinstance(node, dict):
            if len(node) == 1 and "ref" in node and isinstance(node["ref"], str):
                ref_val = node["ref"]
                ref_path = ref_val
                # Resolve relative paths against base_dir
                if not os.path.isabs(ref_path):
                    ref_path = os.path.abspath(os.path.join(base_dir, ref_path))
                logger.info("Resolving ref: %s -> %s", ref_val, ref_path)
                if ref_path in seen_paths:
                    raise ValueError(f"Circular reference detected for {ref_path}")
                seen_paths.add(ref_path)
                # Load referenced file content (without changing working dir)
                referenced = self._load_file_no_sideeffects(ref_path)
                # Resolve refs inside referenced content using its own directory as base
                ref_base_dir = os.path.dirname(ref_path) or base_dir
                resolved_ref = self._resolve_node(copy.deepcopy(referenced), ref_base_dir, seen_paths)
                seen_paths.remove(ref_path)
                return resolved_ref
            else:
                # General dict: resolve each value
                new_dict = {}
                for k, v in node.items():
                    new_dict[k] = self._resolve_node(v, base_dir, seen_paths)
                return new_dict
        elif isinstance(node, list):
            return [self._resolve_node(item, base_dir, seen_paths) for item in node]
        else:
            return node

