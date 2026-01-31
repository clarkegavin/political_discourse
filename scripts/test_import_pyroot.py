import sys
import os
proj_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, proj_root)

from pipelines.boards_discussion_pipeline import BoardsDataPipeline
p = BoardsDataPipeline.from_config({"params": {"category_id": 1728}})
print('OK', type(p))

