import os, sys
sys.path.insert(0, os.getcwd())
import pandas as pd
import numpy as np
from eda.dython_correlation_eda import DythonCorrelationEDA

# create sample df
np.random.seed(1)
df = pd.DataFrame({
    'Id':[1,2,3,4],
    'A':[1,10,100,1000],
    'B':[2,20,200,2000],
    'C':[5,50,500,5000],
    'Date':[pd.Timestamp('2020-01-01'), pd.Timestamp('2020-01-02'), pd.Timestamp('2020-01-03'), pd.Timestamp('2020-01-04')]
})

eda = DythonCorrelationEDA()
out = eda.run(df, save_path='output/eda', exclude_columns=['Id'], filename='test_dython_log.png', log_transform_columns=['A','B'])
print('WROTE', out)

