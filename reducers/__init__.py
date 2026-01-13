# reducers/__init__.py
from .base import Reducer
from .pca_reducer import PCA_Reducer
from .umap_reducer import UMAPReducer
from .factory import ReducerFactory
from .truncatedsvd_reducer import TruncatedSVDReducer

# register default reducers
ReducerFactory.register('umap', UMAPReducer)
ReducerFactory.register('pca', PCA_Reducer)
ReducerFactory.register('truncated_svd', TruncatedSVDReducer)

__all__ = [
    "Reducer",
    "UMAPReducer",
    "PCA_Reducer",
    "ReducerFactory",
    "TruncatedSVDReducer",
]

