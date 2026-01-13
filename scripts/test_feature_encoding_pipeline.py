import os, sys
sys.path.insert(0, os.getcwd())
from pipelines.factory import PipelineFactory
import pandas as pd

yaml_path = 'config/pipelines.yaml'
print('Building pipelines from', yaml_path)
pipelines = PipelineFactory.build_pipelines_from_yaml(yaml_path)
print('Pipelines built:', [p.__class__.__name__ for p in pipelines])

# Find feature encoding pipeline by name in the factory registry
from encoders import OneHotEncoder, MultiHotEncoder
from pipelines.feature_encoder_pipeline import FeatureEncoderPipeline

fe_pipeline = None
for name, inst in PipelineFactory._registry.items():
    if name == 'feature_encoding':
        fe_pipeline = inst
        break

if fe_pipeline is None:
    # fallback: find by class
    for p in pipelines:
        if isinstance(p, FeatureEncoderPipeline):
            fe_pipeline = p
            break

if fe_pipeline is None:
    print('FeatureEncoderPipeline not found; exiting')
    sys.exit(1)

print('Found FeatureEncoderPipeline:', fe_pipeline.__class__.__name__)

# Create sample dataframe matching your config
df = pd.DataFrame({
    'Id': [1,2,3],
    'Platforms': ['Windows, Mac', 'Linux, Windows', 'Mac'],
    'Categories': ['Action, Indie', 'Strategy', 'Action'],
    'Review_Score_Desc': ['Very Positive', 'Mixed', 'Positive'],
    'Type': ['Game','DLC','Game']
})
print('Original columns:', list(df.columns))
print(df.head().to_dict(orient='list'))

# Execute fit_transform
out = fe_pipeline.execute(df)
print('Encoded columns:', list(out.columns))
print(out.head().to_dict(orient='list'))

