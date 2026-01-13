# scripts/run_mask_genre_test.py
import sys
from pathlib import Path
# ensure project root is on sys.path so top-level package imports (e.g. `preprocessing`) work
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from preprocessing.mask_genre_words import MaskGenreWords

# sample data
rows = [
    {"Genre": "Action", "Description": "An action packed shooter with fast action sequences."},
    {"Genre": "All Genres", "Description": "This game spans All Genres and is for everyone."},
    {"Genre": "First Person Shooter", "Description": "A First Person Shooter experience with lots of action."},
    {"Genre": None, "Description": "No genre here, should remain unchanged."},
]

df = pd.DataFrame(rows)
print("Original descriptions:")
print(df['Description'].tolist())

m = MaskGenreWords(genre_field='Genre', description_field='Description', mask_token='<MASKED>')
res = m.transform(df)
print('\nMasked descriptions:')
print(res['Description'].tolist())
