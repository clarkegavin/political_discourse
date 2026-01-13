import mlflow
import pandas as pd
import os
import matplotlib.pyplot as plt
import textwrap

def _set_tracking_uri():
    """Internal helper to set MLflow tracking URI to project_root/mlruns."""
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    tracking_uri = f"file:///{project_root}/mlruns".replace("\\", "/")
    print("Tracking URI:", tracking_uri)
    mlflow.set_tracking_uri(tracking_uri)


def get_last_runs(experiment_name: str, n: int = 9):
    _set_tracking_uri()

    # Get experiment by name
    exp = mlflow.get_experiment_by_name(experiment_name)
    if exp is None:
        raise ValueError(f"Experiment '{experiment_name}' not found")

    # Fetch all runs
    df = mlflow.search_runs(experiment_ids=[exp.experiment_id])
    print(f"Runs found: {len(df)}")

    # Sort newest first and return last n
    df = df.sort_values(by="start_time", ascending=False).head(n)
    return df


def get_runs_by_ids(run_ids: list[str]):
    """
    Retrieve runs directly by their run_id list.
    Returns a single DataFrame containing only those runs.
    """
    _set_tracking_uri()

    # Query ALL runs across ALL experiments (MLflow limitation)
    df_all = mlflow.search_runs()

    # Filter down to only the requested run IDs
    df_filtered = df_all[df_all["run_id"].isin(run_ids)]

    if df_filtered.empty:
        raise ValueError(f"No runs found for provided IDs: {run_ids}")

    print(f"Found {len(df_filtered)} runs matching provided IDs.")
    print(f"Run IDs found: {df_filtered['run_id'].tolist()}")
    return df_filtered


def save_runs_to_excel(df: pd.DataFrame, output_path: str):
    # Remove timezone info for Excel compatibility
    for col in ["start_time", "end_time"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)

    df.to_excel(output_path, index=False)
    print(f"Saved to {output_path}")


def melt_data(df: pd.DataFrame) -> pd.DataFrame:
    """Convert wide-format MLflow output to Tableau-friendly long format."""
    metric_cols = [c for c in df.columns if c.startswith("metrics.")]

    df_long = df.melt(
        id_vars=["run_id", "experiment_id", "status", "artifact_uri", "start_time", "end_time"],
        value_vars=metric_cols,
        var_name="metric",
        value_name="value"
    )

    return df_long

def plot_data(df, output_path: str):
    print("Plotting data...")

    df = df.sort_values("tags.mlflow.runName")

    # Metrics to plot
    metrics = [
        "metrics.test_accuracy",
        "metrics.cv_mean_recall",
        "metrics.test_f1_score",
        "metrics.test_precision"
    ]

    # Extract metric values and convert to percentage
    metric_values = df[metrics].copy() * 100

    # Apply tiny offsets for overlapping metrics (adjust visually as needed)
    offsets = {
        "metrics.test_accuracy": 0.0,
        "metrics.test_recall": 0.1,     # +0.1%
        "metrics.test_f1_score": 0.0,
        "metrics.test_precision": 0.0
    }

    for m in metrics:
        metric_values[m] += offsets.get(m, 0)

    # Compute min/max for dynamic y-axis
    ymin = metric_values.min().min() - 0.2  # 0.2% padding
    ymax = metric_values.max().max() + 0.2

    plt.figure(figsize=(12, 6))

    # Plot each metric
    for m in metrics:

        plt.plot(
            df["tags.mlflow.runName"],
            metric_values[m],
            marker='o',
            label=m.replace("metrics.cv_mean_", "").capitalize()
        )

    plt.xlabel("Experiment Run Name")
    plt.ylabel("Metric Value (%)")
    plt.title("Comparison of  Metrics Across Experiments")
    #plt.xticks(rotation=90)
    wrapped_labels = [
        textwrap.fill(label, width=10) for label in df["tags.mlflow.runName"]
    ]
    plt.xticks(df["tags.mlflow.runName"], wrapped_labels)

    plt.ylim(ymin, ymax)
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.show()

def plot_data_old(df: pd.DataFrame, output_path: str):
    import matplotlib.pyplot as plt
    print("Plotting data...")
    df = df.sort_values("tags.mlflow.runName")

    metrics = [
        "metrics.cv_mean_accuracy",
        "metrics.cv_mean_recall",
        "metrics.cv_mean_f1_score",
        "metrics.cv_mean_precision"
    ]

    metric_values = df[metrics].values.flatten()

    # Determine min/max with small padding
    ymin = metric_values.min() - 0.005  # subtract 0.5%
    ymax = metric_values.max() + 0.005  # add 0.5%


    #print(df.columns.tolist())
    # Set figure size
    plt.figure(figsize=(12, 6))

    for m in metrics:
        plt.plot(df["tags.mlflow.runName"], df[m], marker='o', label=m.replace("metrics.", ""))

    plt.xlabel("Experiment Run Name")
    plt.ylabel("Metric Value")
    plt.title("Comparison of CV Mean Metrics Across Experiments")
    plt.xticks(rotation=45)
    plt.ylim(ymin, ymax)
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.show()





def update_run_names(rename_mapping: dict[str, str]):
    """
    Update MLflow run names given a dictionary of {run_id: new_name}.
    """
    _set_tracking_uri()
    for run_id, new_name in rename_mapping.items():
        mlflow.set_tag(run_id, "mlflow.runName", new_name)
        print(f"Updated run {run_id} → '{new_name}'")

# ======================================================
# Example usage
# ======================================================
if __name__ == "__main__":
    # Option A — get last 9 runs
    # df = get_last_runs("Default", 28)

    # rename_mapping = {
    #     "d4a1480211ef40609bfd311163ff6666": "1a. NB + TF-IDF baseline",
    #     "fe0067b1fb174b3a8d41a3df93f5b981": "1b. NB + lowercase",
    #     "d800a4bb6cd845898bed13cbcd6047a5": "1c. NB + repeated char removal",
    #     "bc413906cf7a4da7ba21c35d7e6258a0": "1d. NB + remove URLs",
    #     "00fba5f8a7ff466aa6fdec0ad73fdc33": "1e. NB + remove whitespace",
    #     "082d1d3a70e644cfb5e8fcbcb515d448": "1f. NB + remove punctuation noise",
    #     "1a38024763854ba1af26ee9a592ac4c6": "1g. NB + stopword remover",
    #     "b996987b18e54ebc86cb221fb7edfa9e": "1h. NB + stemmer (porter)",
    #     "f77543ff914d467ca78077328e7f60ca": "1i. NB + Lemmatization",
    #     "3636caee57d048eb9231b57528286787": "1m. NB + remove repeated characters only",
    #     "dfc90e71f50049878929d68524c63dfa": "1n. NB + remove urls only",
    #     "add63f443ff44a84aa778378f7a7b8c0": "1o. NB + remove whitespace only",
    #     "5c38c47c314b400088259e81b45c2888": "1p. NB + remove punct noise only",
    #     "99d72077c98f4472b19a46918305d82a": "1q. NB + stopword remover only",
    #     "6ad5eb2a745a4c8382e72fc6f6a220b0": "1r. NB + emoji remover only",
    #     "4164603266b74cb38cee7b9188deeb0b": "1s. NB + stemmer (porter) only",
    #     "afdc5387e71448ed90ab4300433cf68c": "1j. NB + Sampling (SMOTE)",
    #     "302e86f2daa24644bbcfebf882c1b3cd": "1k. NB + Sampling (under)",
    #     "2bc2967d2ef545cba04172ef233342f7": "1l. NB + Sampling (over)"
    # }
    # update_run_names(rename_mapping)
    # Option B — get specific runs by ID
    df = get_runs_by_ids([
        # # SVC experiments
        # 'cca653ada0fb4335bc33f4b0ec119959', '28f8623df98e4a74a1602db773d85031', '5e2bf2820d7742f2b8acb4cb74b0e1d0',
        # '12db27bbf21e46309eb27e198228d331', '7f8be5cfad474d268d8893ef72733e15', '3b75b4b5a3e245c4b942c73761443660',
        # '49ccc829d1434bf69ad1265ca81b8024', '2431e391329945ed9846c6b8bb08ae76'

        # XGBoost experiments
        # '3e91943f53794178acc32682d2d00de5', '910effcf9b5a472faf738ef3340dede7', 'da19a43cf23f40d5bcae273764100137',
        # '99ab369e8a8e41babc7ebe0d51b7c00a', '82312831698c4210980338a5353383b1', 'd6f03513dcfa4be6a1a5fe6eb499c160',
        # 'b92ec170012d4aecae5f817a1101dc6f', '87e6ac3985ec4da789693158fe295354', '059bfaf00938450186b2039dc1e67b5b'

        # Random Forest 
        #  '2f3a7027ae10451a8824b2efc37d88bc',
        # '1a7b97bc06694842ac611f5095aa30f5', 'e7b835ba0f93413e97d8850cce53ac1c', '7620429a27d441d6b0015e907a89c6f7',
        # 'ef5d632eaa2f4026b5b254eecde161a8', 'be61981cedff48bd911390cb64cb3af4', 'a942a89dfe1f4da68c060e616e4b9831',
        # '2c48a241b3f1459fa95ccc1ef2172d7b', '2e9bc8528f2d48c697fef6713b0f616d'


        # Linear SVC experiments
        '5764a7485c474d1289d827c291f42a43',
        '4a82b55e5b054e6da75eb6ba556a4d52', '72b4985bfd7f44f29bf93ae983c6eace', '453f36262bca41a3bdf9e57dbecfe09a',
        'cb7098c3615a4a7683b418ff8693766b', '03f7d1f56f8f491a9c51b00a943bb889', 'a0309fbf42414f78abdb9d2919f154b4',
        '4b9f082e5454481fa1095380af39c951', '2068449f6398425f819bcda94d2656c3'

        # '5764a7485c474d1289d827c291f42a43', '4a82b55e5b054e6da75eb6ba556a4d52', '72b4985bfd7f44f29bf93ae983c6eace',
        # '453f36262bca41a3bdf9e57dbecfe09a', 'cb7098c3615a4a7683b418ff8693766b', '03f7d1f56f8f491a9c51b00a943bb889',
        # 'a0309fbf42414f78abdb9d2919f154b4', '4b9f082e5454481fa1095380af39c951', '2068449f6398425f819bcda94d2656c3'

        # Naive Bayes experiments
        #                                                                         '51eab872baa94f0a90f35164eef2e62a',
        # '9e68704941844867ae33aa8d06050e6c', 'c8c9a94835b042959a4728207b37521e',
        # '35b99fcd4c874162840c310db51d0153', 'ef85f6fe24bf46c0a79b6ca826e46487', 'ac276c54b92442529c72fc13ba3a8de5',
        # '0a43743a8bcb4e058562f3b8cb58d96e', '6fe8883434cb4d9ebbd1134e9cf0affa', '36eaad02cdbc40658e67b34551c85762',
        # 'f9ed6a8ab50e47a8bea647eb4d5589ed',
        # '8da79ab2165e4996b6dee69ac4932bbc',
        # '9942c24a588748839a546364b051e7ee',
        # '8238fd201f714a4499acdf951ac69ed1', '575345ff74e2457aba40887240eba91d', '41e43de20c5048c19275665049ed4671',
        # '1a819296449642d48e190ecfc17a1e29', 'a6c4603836024c0789cc02629801e5c9', '4f44240c90264197b4a27be2377e7765',
        # '9374a5d062fe42b2bd9891d3c5d9ac59', '8225f545343c43499ed31220053a5ebd', '4a04e412ace64756ad843cf2df4447e8',
        # '13daf52853964da186e3a5950ba44d19', '8fceeb050d92452bb44cbf7c1d4f5eec', 'd4d8ec8c6c714719a820d7ad4e95ebe0',
        # '96316d79d88b484680b11209a306856d', 'bc9267e17fbf46998c63a23b23d91f6b', 'dc23071204cc44d595352a5b0028bc31',
        # '9242ced92c744912a3bacd69447286b4'
    ])


    # Example: rename runs


    #update_run_names(rename_mapping)

    #df = melt_data(df)
    #df["tags.mlflow.runName"] = df["run_id"].map(rename_mapping)

    # plot_data(df, "xgboost_experiments.png")
    # save_runs_to_excel(df, "xgboost_experiments.xlsx")

    # plot_data(df, "random_forest_experiments.png")
    # save_runs_to_excel(df, "random_forest_experiments.xlsx")

    plot_data(df, "linear_svc_experiments.png")
    save_runs_to_excel(df, "linear_svc_experiments.xlsx")
