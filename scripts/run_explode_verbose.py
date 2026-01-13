import os, sys
# ensure project root is on sys.path
sys.path.insert(0, os.getcwd())

import pandas as pd
from preprocessing.explode_columns import ExplodeColumns

print('CWD:', os.getcwd())
print('PYTHONPATH[0]:', sys.path[0])

df = pd.DataFrame({
    'AppId':[1,2],
    'Platforms':['Windows, Mac', 'Linux, Windows,    Mac'],
    'Name':['GameA','GameB']
})
print('Original DF:\n', df)
exp = ExplodeColumns(columns=['Platforms'], sep=',')
res = exp.transform(df)
print('\nExploded DF:\n', res)

out_path = 'output/eda/explode_test_verbose.csv'
res.to_csv(out_path, index=False)
print('Wrote', out_path)

