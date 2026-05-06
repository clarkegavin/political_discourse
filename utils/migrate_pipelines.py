# utils/migrate_pipelines.py
"""Migration utility: translate legacy config/pipelines.yaml entries into layered config files.

Behavior:
- Reads config/pipelines.yaml (legacy single file)
- For each pipeline entry, writes a pipeline YAML into config/migrated/pipelines/<index>_<name>.yaml
- If a pipeline entry has inline `experiments` (list), each experiment is written to
  config/migrated/experiments/<pipeline_name>_exp<j>.yaml and the pipeline's params.experiments
  list is replaced with [{'ref': 'config/migrated/experiments/...'}]
- Produces a small summary printed to stdout.

This script is idempotent: it will create the migrated directories if missing and will overwrite
files with the same migrated names.
"""
import os
import yaml
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LEGACY_PATH = ROOT / "config" / "pipelines.yaml"
MIG_PIPES_DIR = ROOT / "config" / "migrated" / "pipelines"
MIG_EXPS_DIR = ROOT / "config" / "migrated" / "experiments"

os.makedirs(MIG_PIPES_DIR, exist_ok=True)
os.makedirs(MIG_EXPS_DIR, exist_ok=True)


def _safe_name(name: str) -> str:
    # make a filesystem-safe name
    s = name or "pipeline"
    s = re.sub(r"[^a-zA-Z0-9_-]", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_")


def migrate(legacy_path: Path = None):
    legacy_path = Path(legacy_path) if legacy_path else LEGACY_PATH
    if not legacy_path.exists():
        print(f"Legacy pipeline config not found at {legacy_path}")
        return 0

    with open(legacy_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    pipelines = data.get("pipelines", [])
    if not pipelines:
        print("No pipelines found in legacy config")
        return 0

    created = 0
    for idx, entry in enumerate(pipelines, start=1):
        name = entry.get("name") or entry.get("type") or f"pipeline_{idx}"
        safe = _safe_name(name)
        pipe_filename = f"{idx:02d}_{safe}.yaml"
        pipe_path = MIG_PIPES_DIR / pipe_filename

        # Copy entry and handle experiments
        entry_copy = dict(entry)
        params = entry_copy.get("params", {}) or {}
        experiments = params.get("experiments")
        new_exps_refs = []
        if experiments and isinstance(experiments, list):
            for j, exp in enumerate(experiments, start=1):
                exp_name = exp.get("run_name") or f"{safe}_exp{j}"
                exp_safe = _safe_name(exp_name)
                exp_filename = f"{idx:02d}_{safe}_exp{j}_{exp_safe}.yaml"
                exp_path = MIG_EXPS_DIR / exp_filename
                # Write experiment file
                with open(exp_path, "w", encoding="utf-8") as ef:
                    yaml.safe_dump(exp, ef, sort_keys=False)
                new_exps_refs.append({"ref": str(exp_path.relative_to(ROOT))})

            # Replace experiments list with refs in params
            params = dict(params)
            params["experiments"] = new_exps_refs
            entry_copy["params"] = params

        # Write pipeline file containing a single pipelines list with this entry
        out = {"pipelines": [entry_copy]}
        with open(pipe_path, "w", encoding="utf-8") as pf:
            yaml.safe_dump(out, pf, sort_keys=False)

        created += 1
        print(f"Migrated pipeline '{name}' -> {pipe_path}")

    print(f"Migration complete: {created} pipelines migrated. Experiments written to {MIG_EXPS_DIR}")
    return created


if __name__ == "__main__":
    migrate()

