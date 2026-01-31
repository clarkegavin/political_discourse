#data/data_source/base.py
from abc import ABC, abstractmethod
import pandas as pd

class DiscussionSource(ABC):

    @abstractmethod
    def get_discussions(self) -> "pd.DataFrame":
        """Returns a DataFrame with discussions data."""
        pass

