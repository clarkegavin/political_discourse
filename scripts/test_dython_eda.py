import os, sys
sys.path.insert(0, os.getcwd())
import pandas as pd
from eda.dython_correlation_eda import DythonCorrelationEDA

# create small df
import numpy as np
np.random.seed(0)
df = pd.DataFrame({
    'Id':[1,2,3],
    'AppId':[10,20,30],
    'A':[1,2,3],
    'B':[3,2,1],
    'C':[1,1,2]
})

eda = DythonCorrelationEDA()
out = eda.run(df, save_path='output/eda', exclude_columns=['Id','AppId'], filename='test_dython_corr.png', annot=True)
print('WROTE', out)

