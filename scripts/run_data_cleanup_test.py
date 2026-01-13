# scripts/run_data_cleanup_test.py
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from pipelines.data_cleanup_pipeline import DataCleanupPipeline

# sample data
rows = [
    {"Id": 1, "Title": "Game A", "Genre": "Action"},
    {"Id": 2, "Title": "Game B", "Genre": "Action"},
    {"Id": 3, "Title": "Game C", "Genre": "Puzzle"},
    {"Id": 4, "Title": "Game D", "Genre": None},
    {"Id": 5, "Title": "Game E", "Genre": None},
]

df = pd.DataFrame(rows)
print("Original DataFrame:\n", df)

pipeline_cfg = {
    "cleanup_steps": [
        {"name": "remove_duplicates", "params": {"field": "Genre", "keep": "first", "case_sensitive": False, "dropna": True}}
    ]
}

p = DataCleanupPipeline.from_config(pipeline_cfg)
res = p.execute(df)
print("\nAfter DataCleanupPipeline:\n", res)

