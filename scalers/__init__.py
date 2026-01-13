#scalers/__init__.py
from .factory import ScalerFactory
from .standard_scaler import StandardDataScaler
from .robust_scaler import RobustDataScaler
# register default scalers
ScalerFactory.register('standard', StandardDataScaler)
ScalerFactory.register('robust', RobustDataScaler)

__all__ = [
    "ScalerFactory",
    "StandardDataScaler",
    "RobustDataScaler",
]