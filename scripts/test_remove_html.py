import os, sys
sys.path.insert(0, os.getcwd())
from pipelines.data_cleanup_pipeline import DataCleanupPipeline
import pandas as pd

cfg = {
    'params': {
        'cleanup_steps': [
            {'name': 'remove_html_tags', 'params': {'columns': ['Supported_Languages'], 'br_replace': ', '}},
        ]
    }
}

pipeline = DataCleanupPipeline.from_config(cfg)

df = pd.DataFrame({
    'AppId': [1],
    'Supported_Languages': ["English<strong>*</strong>, French, German<strong>*</strong>, Italian, Spanish - Spain<strong>*</strong>, Czech, Polish, Russian<strong>*</strong><br><strong>*</strong>languages with full audio support"]
})

print('Original:')
print(df['Supported_Languages'].iloc[0])

out = pipeline.execute(df)
print('\nCleaned:')
print(out['Supported_Languages'].iloc[0])

out.to_csv('output/eda/remove_html_test.csv', index=False)
print('\nWROTE output/eda/remove_html_test.csv')

