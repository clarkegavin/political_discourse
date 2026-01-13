import os, sys
sys.path.insert(0, os.getcwd())
import pandas as pd
import numpy as np
from eda.boxplot_eda import BoxPlotEDA

np.random.seed(0)
df = pd.DataFrame({
    'Id': range(1,11),
    'Numeric1': np.random.exponential(scale=100, size=10),
    'Numeric2': np.random.randint(0,1000,size=10),
    'Category': ['A','B','A','B','C','A','C','B','A','C']
})

eda = BoxPlotEDA()
out = eda.run(df, save_path='output/eda', exclude_columns=['Id','Category'], ncols=2, filename='test_boxplots.png', log_transform_columns=['Numeric1'])
print('WROTE', out)

