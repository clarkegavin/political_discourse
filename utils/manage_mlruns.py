#!/usr/bin/env python3
"""Utility to inspect and delete mlflow `mlruns` runs with flexible filters.

Features:
- Select runs by experiment id, run id, regex, missing/empty metric file, or age.
- Support include and exclude lists/regexes.
- Default is dry-run: nothing is deleted unless --force is provided.
- Safe reporting with summary and optional interactive confirmation.

Example usage:
  # Dry-run: show runs missing the metric 'cv_mean_recall'
  python utils/manage_mlruns.py --target-metric cv_mean_recall --delete-if-missing --mlruns-dir ./mlruns

  # Actually delete (force) runs older than 90 days except a protected run
  python utils/manage_mlruns.py --older-than-days 90 --force --exclude-run 12345

"""
import argparse
import os
import re
import shutil
import sys
from datetime import datetime, timedelta
from typing import List, Optional, Pattern
import logging

logger = logging.getLogger("manage_mlruns")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def compile_regex_list(patterns: Optional[List[str]]) -> List[Pattern]:
    if not patterns:
        return []
    out = []
    for p in patterns:
        try:
            out.append(re.compile(p))
        except re.error:
            logger.warning(f"Invalid regex pattern skipped: {p}")
    return out


def matches_any_regex(s: str, regexes: List[Pattern]) -> bool:
    for rx in regexes:
        if rx.search(s):
            return True
    return False


def find_runs(mlruns_dir: str) -> List[tuple]:
    runs = []
    if not os.path.exists(mlruns_dir):
        logger.error(f"mlruns directory does not exist: {mlruns_dir}")
        return runs

    for exp_id in os.listdir(mlruns_dir):
        exp_path = os.path.join(mlruns_dir, exp_id)
        if not os.path.isdir(exp_path):
            continue
        for run_id in os.listdir(exp_path):
            run_path = os.path.join(exp_path, run_id)
            if not os.path.isdir(run_path):
                continue
            runs.append((exp_id, run_id, run_path))
    return runs


def should_delete_run(
    exp_id: str,
    run_id: str,
    run_path: str,
    target_metric: Optional[str],
    delete_if_missing: bool,
    delete_if_metric_empty: bool,
    include_exps: List[str],
    exclude_exps: List[str],
    include_runs: List[str],
    exclude_runs: List[str],
    include_regexes: List[Pattern],
    exclude_regexes: List[Pattern],
    older_than_days: Optional[int],
) -> tuple:
    """Return (should_delete: bool, reason: str)"""
    reasons = []

    metrics_path = os.path.join(run_path, "metrics")
    metric_file = None
    metric_missing = False
    metric_empty = False
    if target_metric:
        metric_file = os.path.join(metrics_path, target_metric)
        if not os.path.exists(metric_file):
            metric_missing = True
        else:
            try:
                if os.path.getsize(metric_file) == 0:
                    metric_empty = True
            except OSError:
                metric_empty = False

    # age
    older = False
    if older_than_days is not None:
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(run_path))
            cutoff = datetime.now() - timedelta(days=older_than_days)
            older = mtime < cutoff
        except OSError:
            older = False

    # start with False and turn on if any include conditions match
    should_delete = False

    if delete_if_missing and metric_missing:
        should_delete = True
        reasons.append("missing_metric")
    if delete_if_metric_empty and metric_empty:
        should_delete = True
        reasons.append("empty_metric")
    if include_exps and exp_id in include_exps:
        should_delete = True
        reasons.append("include_exp")
    if include_runs and run_id in include_runs:
        should_delete = True
        reasons.append("include_run")
    if include_regexes and matches_any_regex(run_path, include_regexes):
        should_delete = True
        reasons.append("include_regex")
    if older_than_days is not None and older:
        should_delete = True
        reasons.append(f"older_than_{older_than_days}d")

    # exclusions override inclusions
    if exp_id in exclude_exps:
        return False, "excluded_exp"
    if run_id in exclude_runs:
        return False, "excluded_run"
    if exclude_regexes and matches_any_regex(run_path, exclude_regexes):
        return False, "exclude_regex"

    return should_delete, ";".join(reasons) if reasons else "none"


def delete_runs(selected_runs: List[tuple], dry_run: bool, force: bool) -> List[str]:
    deleted = []
    for exp_id, run_id, run_path, reason in selected_runs:
        if dry_run:
            logger.info(f"[DRY-RUN] Would delete {run_path} (reason={reason})")
            continue
        if not force:
            logger.info(f"Skipping actual delete for {run_path} because --force not provided")
            continue
        try:
            logger.info(f"Deleting {run_path} (reason={reason})")
            shutil.rmtree(run_path)
            deleted.append(run_path)
        except Exception as e:
            logger.error(f"Failed to delete {run_path}: {e}")
    return deleted


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Manage and delete mlruns with flexible filters (safe by default)")
    p.add_argument("--mlruns-dir", default=None, help="Path to mlruns dir (default: project_root/mlruns)")
    p.add_argument("--target-metric", default=None, help="Metric filename to check inside <run>/metrics/")
    p.add_argument("--delete-if-missing", action="store_true", help="Mark runs for deletion if the target metric is missing")
    p.add_argument("--delete-if-metric-empty", action="store_true", help="Mark runs for deletion if the target metric exists but is empty (0 bytes)")
    p.add_argument("--include-exp", action="append", default=[], help="Experiment id to include (repeatable)")
    p.add_argument("--exclude-exp", action="append", default=[], help="Experiment id to exclude (repeatable)")
    p.add_argument("--include-run", action="append", default=[], help="Run id to include (repeatable)")
    p.add_argument("--exclude-run", action="append", default=[], help="Run id to exclude (repeatable)")
    p.add_argument("--include-regex", action="append", default=[], help="Regex to include matching run paths (repeatable)")
    p.add_argument("--exclude-regex", action="append", default=[], help="Regex to exclude matching run paths (repeatable)")
    p.add_argument("--older-than-days", type=int, default=None, help="Mark runs older than this many days for deletion")
    p.add_argument("--dry-run", dest="dry_run", action="store_true", default=True, help="Default: do not delete; show what would be deleted")
    p.add_argument("--no-dry-run", dest="dry_run", action="store_false", help="Allow deletions (requires --force to actually delete)")
    p.add_argument("--force", action="store_true", help="Actually remove selected runs (use with care) ")
    p.add_argument("--yes", action="store_true", help="Assume yes for interactive prompts")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    # determine mlruns_dir default (project root is parent of utils)
    mlruns_dir = args.mlruns_dir
    if not mlruns_dir:
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        mlruns_dir = os.path.join(project_root, "mlruns")

    logger.info(f"Using mlruns dir: {mlruns_dir}")

    include_regexes = compile_regex_list(args.include_regex)
    exclude_regexes = compile_regex_list(args.exclude_regex)

    runs = find_runs(mlruns_dir)
    logger.info(f"Found {len(runs)} runs (exp/run pairs) to consider")

    selected = []
    for exp_id, run_id, run_path in runs:
        should_delete, reason = should_delete_run(
            exp_id=exp_id,
            run_id=run_id,
            run_path=run_path,
            target_metric=args.target_metric,
            delete_if_missing=args.delete_if_missing,
            delete_if_metric_empty=args.delete_if_metric_empty,
            include_exps=args.include_exp,
            exclude_exps=args.exclude_exp,
            include_runs=args.include_run,
            exclude_runs=args.exclude_run,
            include_regexes=include_regexes,
            exclude_regexes=exclude_regexes,
            older_than_days=args.older_than_days,
        )
        if should_delete:
            selected.append((exp_id, run_id, run_path, reason))

    if not selected:
        logger.info("No runs matched deletion criteria. Nothing to do.")
        return 0

    logger.info(f"{len(selected)} runs selected for deletion (dry_run={args.dry_run}). Sample:")
    for i, (_, _, path, reason) in enumerate(selected[:20]):
        logger.info(f"  - {path} (reason={reason})")
    if len(selected) > 20:
        logger.info(f"  ... and {len(selected)-20} more")

    if args.dry_run:
        logger.info("Dry-run mode enabled; no runs will be deleted. Use --no-dry-run and --force to perform deletion.")
    else:
        if not args.force:
            logger.info("--no-dry-run used but --force not provided: nothing will be deleted. Provide --force to actually remove runs.")
        else:
            if not args.yes:
                # ask for confirmation
                try:
                    ans = input(f"Are you sure you want to DELETE {len(selected)} runs under {mlruns_dir}? Type 'yes' to proceed: ")
                except KeyboardInterrupt:
                    logger.info("Aborted by user")
                    return 1
                if ans.strip().lower() != "yes":
                    logger.info("User did not confirm; aborting")
                    return 1

    deleted = delete_runs(selected, dry_run=args.dry_run, force=args.force)

    logger.info(f"Deleted {len(deleted)} runs (requested {len(selected)})")
    for d in deleted:
        logger.info(f"  - {d}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

