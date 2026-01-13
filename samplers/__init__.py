from .base import Sampler

from .over_sample import OverSampler
from .under_sample import UnderSampler
from .smote import SMOTESampler
from .composite_sampler import CompositeSampler
from .factory import SamplerFactory


SamplerFactory.register_sampler('over_sampler', OverSampler)
SamplerFactory.register_sampler('over', OverSampler)
SamplerFactory.register_sampler('under_sampler', UnderSampler)
SamplerFactory.register_sampler('under', UnderSampler)
SamplerFactory.register_sampler('smote_sampler', SMOTESampler)
SamplerFactory.register_sampler('smote', SMOTESampler)
SamplerFactory.register_sampler('composite_sampler', CompositeSampler)
SamplerFactory.register_sampler('composite', CompositeSampler)

__all__ = [
    "Sampler",
    "OverSampler",
    "UnderSampler",
    "SMOTESampler",
    "SamplerFactory",
    "CompositeSampler",
]