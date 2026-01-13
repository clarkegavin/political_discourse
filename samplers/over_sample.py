from imblearn.over_sampling import RandomOverSampler
from .base import Sampler
from logs.logger import get_logger

class OverSampler(Sampler):
    """
    Wrapper for RandomOverSampler from imblearn.
    """

    def __init__(self, name: str = "over_sampler", **kwargs):
        self.name = name
        self.sampler = RandomOverSampler(**kwargs)
        self.logger = get_logger("OverSampler")
        self.logger.info(f"Initialized OverSampler with name: {self.name} and params: {kwargs}")

    def fit_resample(self, X, y):
        self.logger.info("Starting OverSampler fit_resample")
        # if X is a DataFrame, log its shape otherwise log its length
        # X_res, y_res = self.sampler.fit_resample(X, y)

        # if hasattr(X, 'shape'):
        #     self.logger.info(f"OverSampler Input X shape: {X.shape}")
        #     self.logger.info(f"OverSampler Resampled X shape: {X_res.shape}")
        # else:
        #     self.logger.info(f"OverSampler Input X length: {len(X)}")
        #     self.logger.info(f"OverSampler Resampled X length: {len(X_res)}")

        X_res, y_res = self.sampler.fit_resample(X, y)

        return X_res, y_res

        #return self.sampler.fit_resample(X, y)
