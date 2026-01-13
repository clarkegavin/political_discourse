# preprocessing/sequential.py
from logs.logger import get_logger

class SequentialPreprocessor:
    """
    Simple wrapper to apply a list of preprocessors sequentially.
    Each preprocessor must implement fit_transform and transform.
    """
    def __init__(self, steps):
        self.logger = get_logger("SequentialPreprocessor")
        self.logger.info(f"Initialized with {len(steps)} steps.")
        self.steps = steps

    def fit_transform(self, X):
        for step in self.steps:
            self.logger.info(f"Applying preprocessor: {step.__class__.__name__}")
            X = step.fit_transform(X)
            self.logger.info(f"Completed preprocessor: {step.__class__.__name__}")
            self.logger.info(f"Data type after preprocessor fit transform: {type(X)}")
        return X

    def transform(self, X):
        for step in self.steps:
            self.logger.info(f"Applying preprocessor: {step.__class__.__name__}")
            X = step.transform(X)
            self.logger.info(f"Completed preprocessor: {step.__class__.__name__}")
            self.logger.info(f"Data type after preprocessor transform: {type(X)}")
        return X
