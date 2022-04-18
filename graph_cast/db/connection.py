import logging
from graph_cast.db.abstract_config import ConnectionConfig
from typing import TypeVar
import abc

logger = logging.getLogger(__name__)


ConnectionType = TypeVar("ConnectionType", bound="Connection")


class Connection(abc.ABC):
    def __init__(self, config: ConnectionConfig):
        pass
