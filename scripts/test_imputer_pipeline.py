from pathlib import Path
import sys
# Ensure project root is on sys.path
root = Path(__file__).resolve().parents[1]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

from pipelines.factory import PipelineFactory
import pandas as pd

pipelines = PipelineFactory.build_pipelines_from_yaml('config/pipelines.yaml')

imputer = None
for p in pipelines:
    if p.__class__.__name__ == 'ImputerPipeline' or getattr(p, 'name', None) == 'imputation':
        imputer = p
        break

print('Found imputer pipeline:', imputer)

if imputer is None:
    print('No imputer pipeline configured')
else:
    data = {
        'Description': ['This is fine', None, 'Another desc', None],
        'Current_Players': [100, None, 200, None],
        'Other': [None, None, None, None]
    }
    df = pd.DataFrame(data)
    print('Before:')
    print(df)
    out = imputer.execute(df)
    print('\nAfter:')
    print(out)

