from .factory import DataSaverFactory
from .sql_server_saver import SQLServerSaver

# Register default savers
DataSaverFactory.register_saver("sql_server", SQLServerSaver)

__all__ = [
    "DataSaverFactory",
    "SQLServerSaver",
]

