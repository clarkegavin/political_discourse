import os, sys
sys.path.insert(0, os.getcwd())
import pandas as pd
from pipelines.factory import PipelineFactory

# build pipelines
pipelines = PipelineFactory.build_pipelines_from_yaml('config/pipelines.yaml')
# find data_cleanup pipeline in factory
from pipelines.data_cleanup_pipeline import DataCleanupPipeline

data_cleanup = None
for p in pipelines:
    if isinstance(p, DataCleanupPipeline):
        data_cleanup = p
        break

if data_cleanup is None:
    print('data_cleanup pipeline not found')
    sys.exit(1)

# sample df
import numpy as np
np.random.seed(0)
df = pd.DataFrame({
    'Current_Players': [0, 10, 100, -5, None],
    'Total_Reviews': [1, 5, 100, 200, 0],
    'Total_Positive': [1,2,3,4,5],
    'Total_Negative': [0,0,1,2,3],
    'Recommendations': [10, 20, 30, 40, 50],
    'Supported_Languages': ['English<strong>*</strong>', None, 'German<strong>*</strong>', 'Italian', 'Spanish']
})
print('Before:')
print(df)

out = data_cleanup.execute(df)
print('After:')
print(out)
print('Columns:', list(out.columns))

