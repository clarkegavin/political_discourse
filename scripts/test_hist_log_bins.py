import os, sys
sys.path.insert(0, os.getcwd())
import pandas as pd
from eda.class_balance_eda import ClassBalanceEDA

# create sample df
import numpy as np
np.random.seed(0)
df = pd.DataFrame({
    'AppId': range(1,11),
    'Current_Players': np.concatenate([np.random.exponential(scale=1000, size=8), [0, -5]]),
    'Category': ['A','B','A','B','C','A','C','B','A','C']
})

cb = ClassBalanceEDA()
out = cb.run(df, target=None, save_path='output/eda', log_transform_columns=['Current_Players'], hist_bins={'Current_Players':5}, exclude_columns=['AppId'])
print('Saved to', out)

