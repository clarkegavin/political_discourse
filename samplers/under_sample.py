# samplers/under_sample.py
from imblearn.under_sampling import RandomUnderSampler
from samplers.factory import SamplerFactory
from logs.logger import get_logger
from sklearn.preprocessing import LabelEncoder

class UnderSampler:
    """
    Wrapper for RandomUnderSampler from imblearn.
    """
    def __init__(self, name: str = "under_sampler", **kwargs):
        self.name = name
        self.sampler = RandomUnderSampler(**kwargs)
        self.logger = get_logger("UnderSampler")
        self.logger.info(f"Initialized UnderSampler with name: {self.name} and params: {kwargs}")

    def fit_resample(self, X, y):
        self.logger.info(f"y unique classes before resampling: {set(y)}")
        return self.sampler.fit_resample(X, y)

