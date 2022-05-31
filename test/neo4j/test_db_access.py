import unittest
from os.path import join, dirname, realpath
import logging
import sys
from graph_cast.db.arango.util import insert_return_batch

from graph_cast.db import ConnectionManager, ConfigFactory

logger = logging.getLogger(__name__)


class TestDBAccess(unittest.TestCase):
    cpath = dirname(realpath(__file__))

    db_args = {
        "protocol": "bolt",
        "ip_addr": "localhost",
        "port": 7687,
        "cred_name": "neo4j",
        "cred_pass": "test",
        # "database": "root",
        "db_type": "neo4j",
    }

    def test_db_access(self):
        db_args = dict(self.db_args)
        conn_conf = ConfigFactory.create_config(args=db_args)

        with ConnectionManager(connection_config=conn_conf) as db_client:
            query = "MATCH(n) RETURN n LIMIT 3"
            result = db_client.execute(query, params=None)
            r = [record for record in result]

    # def test_insert_return(self):
    #     db_args = dict(self.db_args)
    #     db_args["database"] = "wos_test"
    #     conn_conf = ConfigFactory.create_config(args=db_args)
    #     with ConnectionManager(connection_config=conn_conf) as db_client:
    #         cnames = [
    #             c["name"] for c in db_client.get_collections() if c["name"][0] != "_"
    #         ]
    #
    #     docs = [{"value": i} for i in range(5)]
    #     query0 = insert_return_batch(docs, "test")
    #
    #     cursor = db_client.execute(query0)
    #     for item in cursor:
    #         logger.info(item)
    #
    # def test_query(self):
    #     db_args = dict(self.db_args)
    #     db_args["database"] = "ibes_test"
    #     conn_conf = ConfigFactory.create_config(args=db_args)
    #
    #     q = """for doc in analysts limit 5 return doc
    #     """
    #
    #     with ConnectionManager(connection_config=conn_conf) as db_client:
    #         cursor = db_client.execute(q)
    #     chunk = list(cursor.batch())
    #     logger.info(chunk)


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    unittest.main()
