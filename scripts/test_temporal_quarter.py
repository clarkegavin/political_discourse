from pathlib import Path
import sys
root = Path(__file__).resolve().parents[1]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

import pandas as pd
from preprocessing.temporal_features import TemporalFeatures

# Sample data
df = pd.DataFrame({'ReleaseDate': ['2025-12-21', '2025-01-15', None, '2025-04-01']})
print('Input:')
print(df)

# Instantiate with quarter=True
tf = TemporalFeatures(date_column='ReleaseDate', day=False, month=False, year=False, quarter=True)
out = tf.transform(df)
print('\nOutput (label):')
print(out)

# Now test integer quarter format
tf_int = TemporalFeatures(date_column='ReleaseDate', day=False, month=False, year=False, quarter=True, quarter_format='int')
out_int = tf_int.transform(df)
print('\nOutput (int):')
print(out_int)
