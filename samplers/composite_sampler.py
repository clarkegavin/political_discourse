# samplers/composite_sampler.py

from samplers.base import Sampler
from samplers.factory import SamplerFactory
from logs.logger import get_logger

class CompositeSampler(Sampler):
    def __init__(self, steps=None, **kwargs):  # <-- accept extra kwargs
        self.steps_config = steps or []
        self.steps = []
        self.logger = get_logger("CompositeSampler")

        # build samplers from YAML config
        for step in self.steps_config:
            name = step["name"]
            params = step.get("params", {})
            self.logger.info(f"Adding sampler step: {name} with params: {params}")
            sampler = SamplerFactory.get_sampler(name, **params)
            self.logger.info(f"Sampler instance created: {sampler}")
            self.steps.append(sampler)


    def fit_resample(self, X, y):
        self.logger.info("Starting composite fit_resample")
        for sampler in self.steps:
            self.logger.info(f"Applying sampler: {sampler.name}")
            X, y = sampler.fit_resample(X, y)
        return X, y
