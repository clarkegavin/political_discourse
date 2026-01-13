# eda/base.py
from abc import ABC, abstractmethod

class EDAComponent(ABC):
    """
    Abstract base class for EDA components.
    """
    @abstractmethod
    def run(self, data, target, text_field, save_path, **kwargs):
        """
        Perform the EDA analysis.
        """
        pass