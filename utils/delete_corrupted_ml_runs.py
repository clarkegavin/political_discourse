import os
import shutil

# get project root based on script location
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
MLRUNS_DIR = os.path.join(PROJECT_ROOT, "mlruns")  # now points to root/mlruns

TARGET_METRIC = "cv_mean_recall"

deleted_runs = []

for exp_id in os.listdir(MLRUNS_DIR):
    exp_path = os.path.join(MLRUNS_DIR, exp_id)
    if not os.path.isdir(exp_path):
        continue

    for run_id in os.listdir(exp_path):
        run_path = os.path.join(exp_path, run_id)
        metrics_path = os.path.join(run_path, "metrics")
        if not os.path.exists(metrics_path):
            continue

        metric_file = os.path.join(metrics_path, TARGET_METRIC)
        if not os.path.exists(metric_file) or os.path.getsize(metric_file) == 0:
            print(f"Deleting corrupted run: {run_path}")
            shutil.rmtree(run_path)
            deleted_runs.append(run_path)

print(f"Deleted {len(deleted_runs)} runs.")
