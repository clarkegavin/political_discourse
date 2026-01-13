from pathlib import Path
import sys
root = Path(__file__).resolve().parents[1]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

from pipelines.factory import PipelineFactory
import pandas as pd

pipelines = PipelineFactory.build_pipelines_from_yaml('config/pipelines.yaml')
pre = None
for p in pipelines:
    if p.__class__.__name__ == 'PreprocessingPipeline' or getattr(p, 'name', None) == 'preprocessing':
        pre = p
        break

print('Got preprocessing pipeline:', pre)

if pre is None:
    print('Preprocessing not found')
else:
    data = {
        'Publishers': ['Ritual Entertainment,Nightdive Studios', None, '', 'Studio A, Studio B, Studio C'],
        'Developers': ['Dev1', 'Dev1, Dev2', None, '']
    }
    df = pd.DataFrame(data)
    print('Before:')
    print(df)
    out = pre.execute(df)
    print('\nAfter:')
    print(out)

