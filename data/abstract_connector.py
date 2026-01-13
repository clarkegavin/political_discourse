#data/abstract_connector.py
from abc import ABC, abstractmethod
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session


class DBConnector(ABC):
    """Abstract connector: contract for obtaining engine and sessions.

    Implementations must provide an SQLAlchemy Engine and a Session factory.
    """

    @abstractmethod
    def get_engine(self) -> Engine:
        raise NotImplementedError

    @abstractmethod
    def get_session(self) -> Session:
        raise NotImplementedError

