import sys
import os
proj_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, proj_root)

from pipelines.boards_pipeline import BoardsDataPipeline

p = BoardsDataPipeline(category_id=1728, limit=5, date_start="2025-01-01", date_end=None)
df = p.execute()
print('Fetched rows:', len(df))
print(df.head().to_dict(orient='records'))

