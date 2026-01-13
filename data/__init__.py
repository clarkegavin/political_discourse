#data/__init__.py
"""Data package exports for the project's data layer.

Expose a simple factory entry-point so callers can get extractors like:

from data import ExtractorFactory
extractor = ExtractorFactory.create_roblox_extractor()
"""

from .factory import ExtractorFactory

__all__ = ["ExtractorFactory"]

