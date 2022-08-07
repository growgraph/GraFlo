import logging
import sys
import unittest
from os.path import dirname, realpath

from graph_cast.db import ConfigFactory, ConnectionManager
from graph_cast.db.arango.util import insert_return_batch

logger = logging.getLogger(__name__)


class TestDBAccess(unittest.TestCase):
    cpath = dirname(realpath(__file__))

    db_args = {
        "protocol": "bolt",
        "ip_addr": "localhost",
        "port": 7687,
        "cred_name": "neo4j",
        "cred_pass": "test",
        "db_type": "neo4j",
    }

    @unittest.skip("")
    def test_db_access(self):
        db_args = dict(self.db_args)
        conn_conf = ConfigFactory.create_config(args=db_args)

        with ConnectionManager(connection_config=conn_conf) as db_client:
            query = "MATCH(n) RETURN n LIMIT 3"
            result = db_client.execute(query, params=None)
            [record for record in result]


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    unittest.main()
