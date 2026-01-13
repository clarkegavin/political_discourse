from pathlib import Path
import sys
# Ensure project root is on sys.path
root = Path(__file__).resolve().parents[1]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

from pipelines.factory import PipelineFactory
import pandas as pd

# Build pipelines from config
pipelines = PipelineFactory.build_pipelines_from_yaml('config/pipelines.yaml')

# Find preprocessing pipeline
pre = None
for p in pipelines:
    if p.__class__.__name__ == 'PreprocessingPipeline' or getattr(p, 'name', None) == 'preprocessing':
        pre = p
        break

print('Found preprocessing pipeline:', pre)

if pre is None:
    print('No preprocessing pipeline configured; exiting')
else:
    # Create sample DataFrame
    data = {
        'ReleaseDate': ['2020-01-01', '2019-06-15', None],
        'Description': ['Running FAST!!!', 'An example description', None]
    }
    df = pd.DataFrame(data)
    print('Before:', df.to_dict(orient='list'))
    out = pre.execute(df)
    print('After columns:', out.columns.tolist())
    print(out.head().to_dict(orient='list'))
