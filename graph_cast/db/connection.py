import logging
from graph_cast.db.abstract_config import ConnectionConfig
import abc

logger = logging.getLogger(__name__)


class Connection(abc.ABC):
    def __init__(self, config: ConnectionConfig):
        pass
