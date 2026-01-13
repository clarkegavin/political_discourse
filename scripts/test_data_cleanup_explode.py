import os, sys
sys.path.insert(0, os.getcwd())
from pipelines.data_cleanup_pipeline import DataCleanupPipeline
import pandas as pd

cfg = {
    'params': {
        'cleanup_steps': [
            {'name': 'explode_columns', 'params': {'columns': ['Platforms'], 'sep': ','}},
        ]
    }
}

pipeline = DataCleanupPipeline.from_config(cfg)

# sample df
df = pd.DataFrame({
    'AppId':[1,2],
    'Platforms':['Windows, Mac', 'Linux, Windows,    Mac'],
    'Name':['GameA','GameB']
})

print('Original shape:', df.shape)
print(df)

out = pipeline.execute(df)
print('Result shape:', out.shape)
print(out)

out.to_csv('output/eda/data_cleanup_explode.csv', index=False)
print('WROTE output/eda/data_cleanup_explode.csv')

