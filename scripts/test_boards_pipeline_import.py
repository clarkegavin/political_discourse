from pipelines.boards_pipeline import BoardsDataPipeline
p = BoardsDataPipeline.from_config({"params": {"category_id": 1728}})
print('OK', type(p))

