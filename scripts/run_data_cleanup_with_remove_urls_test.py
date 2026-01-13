# scripts/run_data_cleanup_with_remove_urls_test.py
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from pipelines.data_cleanup_pipeline import DataCleanupPipeline

rows = [
    {"Id": 1, "Title": "Game A", "Genre": "Action", "Description": "Check this out: https://example.com/play/1"},
    {"Id": 2, "Title": "Game B", "Genre": "Action", "Description": "Visit www.example.org for more info"},
    {"Id": 3, "Title": "Game C", "Genre": "Puzzle", "Description": "No urls here, just text."},
    {"Id": 4, "Title": "Game D", "Genre": None, "Description": "https://short.url/a"},
    {"Id": 5, "Title": "Game E", "Genre": None, "Description": "Text and http://another.example/test"},
]

df = pd.DataFrame(rows)
print("Original descriptions:")
print(df[['Id','Description']].to_string(index=False))

pipeline_cfg = {
    "cleanup_steps": [
        {"name": "remove_urls", "params": {"field": "Description"}},
        {"name": "remove_duplicates", "params": {"field": "Genre", "keep": "first", "case_sensitive": False, "dropna": True}}
    ]
}

p = DataCleanupPipeline.from_config(pipeline_cfg)
res = p.execute(df)

print('\nAfter DataCleanupPipeline:')
print(res[['Id','Description','Genre']].to_string(index=False))

