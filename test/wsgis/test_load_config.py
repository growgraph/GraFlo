import logging
import sys
import unittest
from os.path import dirname, realpath

from graph_cast.db import ConfigFactory, ConnectionManager
from graph_cast.db.arango.util import insert_return_batch

logger = logging.getLogger(__name__)


class TestDBAccess(unittest.TestCase):
    cpath = dirname(realpath(__file__))

    conf = {
        "comment": "empty",
        "protocol": "http",
        "ip_addr": "localhost",
        "port": 333,
        "source_db": "sql",
        "db_type": "wsgi",
        "path": "/gg",
        "paths": {"navigate": "/gg", "trends": "/trending"},
        "host": "0.0.0.0",
    }

    def test_config(self):
        wsgi_self_obj = ConfigFactory.create_config(args=self.conf)
        print(wsgi_self_obj.paths)


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    unittest.main()
