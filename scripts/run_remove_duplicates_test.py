# scripts/run_remove_duplicates_test.py
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from preprocessing.remove_duplicates import RemoveDuplicates

# DataFrame test
rows = [
    {"Id": 1, "Title": "Game A", "Genre": "Action"},
    {"Id": 2, "Title": "Game B", "Genre": "Action"},
    {"Id": 3, "Title": "Game C", "Genre": "Puzzle"},
    {"Id": 4, "Title": "Game D", "Genre": None},
    {"Id": 5, "Title": "Game E", "Genre": None},
]

df = pd.DataFrame(rows)
print("Original DataFrame:\n", df)

# remove duplicates on Genre, keep first, dropna True (None considered as value and deduped)
r = RemoveDuplicates(field='Genre', keep='first', case_sensitive=False, dropna=True)
res_df = r.transform(df)
print("\nAfter RemoveDuplicates (dropna=True, keep=first):\n", res_df)

# iterable dict test
items = [
    {"Id": 1, "Genre": "Action"},
    {"Id": 2, "Genre": "action"},
    {"Id": 3, "Genre": "Puzzle"},
    {"Id": 4, "Genre": None},
    {"Id": 5, "Genre": None},
]

r2 = RemoveDuplicates(field='Genre', keep='first', case_sensitive=False, dropna=True)
res_items = r2.transform(items)
print("\nAfter RemoveDuplicates on iterable (case-insensitive):\n", res_items)

# keep last example
r3 = RemoveDuplicates(field='Genre', keep='last', case_sensitive=False, dropna=True)
res_items_last = r3.transform(items)
print("\nAfter RemoveDuplicates on iterable (keep=last):\n", res_items_last)

