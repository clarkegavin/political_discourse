import os, sys
sys.path.insert(0, os.getcwd())
import pandas as pd
from pipelines.factory import PipelineFactory

# Build pipelines
pipelines = PipelineFactory.build_pipelines_from_yaml('config/pipelines.yaml')

# Find data_cleanup pipeline
from pipelines.data_cleanup_pipeline import DataCleanupPipeline

cleanup = None
for p in pipelines:
    if isinstance(p, DataCleanupPipeline):
        cleanup = p
        break

if cleanup is None:
    print('DataCleanupPipeline not found in pipelines')
    raise SystemExit(1)

# Prepare sample data
df = pd.DataFrame({
    'AppId': [1,2,3,4],
    'ReleaseDate': ['2020-01-15', '2019-06-30', 'invalid date', None]
})
print('Before:')
print(df)

out = cleanup.execute(df)
print('After:')
print(out)
print('Columns:', list(out.columns))

