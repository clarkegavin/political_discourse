#samplers/smote.py
from imblearn.over_sampling import SMOTE
from samplers.factory import SamplerFactory
from logs.logger import get_logger

class SMOTESampler:
    """
    Wrapper for SMOTE from imblearn.
    """
    def __init__(self, name: str = "smote_sampler", **kwargs):
        self.logger = get_logger("SMOTESampler")
        self.name = name
        self.sampler = SMOTE(**kwargs)

    def fit_resample(self, X, y):
        self.logger.info("Starting SMOTE fit_resample")
        return self.sampler.fit_resample(X, y)
