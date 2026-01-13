#filters/base.py
from abc import ABC, abstractmethod
from logs.logger import get_logger
import pandas as pd


class Filter(ABC):
    """
    Abstract base class for data filters.
    """
    @abstractmethod
    def fit(self, data: pd.DataFrame):
        pass

    @abstractmethod
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        pass

    def fit_transform(self, data: pd.DataFrame) -> pd.DataFrame:
        self.fit(data)
        return self.transform(data)

