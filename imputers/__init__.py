# imputers/__init__.py
from .base import Imputer
from .factory import ImputerFactory
from .simple_imputer import SimpleImputer

# Register default imputers
ImputerFactory.register('simple', SimpleImputer)
ImputerFactory.register('simple_imputer', SimpleImputer)

__all__ = ['Imputer', 'ImputerFactory', 'SimpleImputer']

