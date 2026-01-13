#fetcher/base.py
from abc import ABC, abstractmethod
from fetchers.context import FetcherContext

class Fetcher(ABC):
    """Abstract api fetcher interface.

    """
    def __init__(self, context: FetcherContext):
        self.context = context

    @abstractmethod
    def fetch(self, *args, **kwargs) -> dict:
        """fetch data from api endpoint."""
        pass