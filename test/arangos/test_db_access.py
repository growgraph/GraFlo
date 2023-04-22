import logging
import sys
import unittest
from os.path import dirname, realpath

from graph_cast.architecture import JConfigurator
from graph_cast.db import ConfigFactory, ConnectionManager
from graph_cast.db.arango.util import (
    fetch_fields,
    insert_edges_batch,
    insert_return_batch,
    upsert_docs_batch,
)
from graph_cast.db.connection import init_db
from graph_cast.util import ResourceHandler

logger = logging.getLogger(__name__)


class TestDBAccess(unittest.TestCase):
    cpath = dirname(realpath(__file__))

    db_args = {
        "protocol": "http",
        "ip_addr": "127.0.0.1",
        "port": 8529,
        "cred_name": "test",
        "cred_pass": "123",
        "database": "testdb",
        "db_type": "arango",
    }

    @unittest.skip("")
    def test_db_access(self):
        db_args = dict(self.db_args)
        db_args["database"] = "testdb"
        conn_conf = ConfigFactory.create_config(args=db_args)
        config = ResourceHandler.load(f"conf.json", f"lake_odds.yaml")
        conf_obj = JConfigurator(config)

        with ConnectionManager(connection_config=conn_conf) as db_client:
            init_db(db_client, conf_obj, clean_start=True)
            cnames = [
                c["name"]
                for c in db_client.get_collections()
                if c["name"][0] != "_"
            ]
            for c in cnames:
                logger.info(c)

        with ConnectionManager(connection_config=conn_conf) as db_client:
            init_db(db_client, conf_obj, clean_start=False)
            cnames = [
                c["name"]
                for c in db_client.get_collections()
                if c["name"][0] != "_"
            ]
            for c in cnames:
                logger.info(c)

    @unittest.skip("")
    def test_edges_upsert(self):
        config = ResourceHandler.load(f"conf.json", f"kg_v3.yaml")
        conf_obj = JConfigurator(config)

        db_args = dict(self.db_args)
        db_args["database"] = "testdb"
        conn_conf = ConfigFactory.create_config(args=db_args)

        with ConnectionManager(connection_config=conn_conf) as db_client:
            init_db(db_client, conf_obj, clean_start=True)

        vs = [{"hash": "aaa"}, {"hash": "bbb"}]
        es = [
            {
                "__source": {"hash": "aaa"},
                "__target": {"hash": "bbb"},
                "publication": {"doi": "10.1101/cc"},
            },
            {
                "__source": {"hash": "aaa"},
                "__target": {"hash": "bbb"},
                "publication": {"doi": "10.1101/cc"},
            },
            {
                "__source": {"hash": "aaa"},
                "__target": {"hash": "bbb"},
                "publication": {"doi": "10.1101/dd"},
            },
        ]

        es2 = [
            {
                "__source": {"hash": "aaa"},
                "__target": {"hash": "bbb"},
                "publication": {"doi": "10.1101/cc"},
            },
            {
                "__source": {"hash": "aaa"},
                "__target": {"hash": "bbb"},
                "publication": {"doi": "10.1101/cc"},
            },
            {
                "__source": {"hash": "aaa"},
                "__target": {"hash": "bbb"},
                "publication": {"doi": "10.1101/dd"},
            },
        ]

        source_col = "mentions"
        target_col = "mentions"
        edge_col_name = "mentions_mentions_edges"
        match_keys_source = ["hash"]
        match_keys_target = ["hash"]

        with ConnectionManager(connection_config=conn_conf) as db_client:
            q = upsert_docs_batch(vs, "mentions", match_keys=["hash"])
            db_client.execute(q)
            q_edges = insert_edges_batch(
                es,
                source_col,
                target_col,
                edge_col_name=edge_col_name,
                match_keys_source=match_keys_source,
                match_keys_target=match_keys_target,
            )
            db_client.execute(q_edges)

            q_edges2 = insert_edges_batch(
                es2,
                source_col,
                target_col,
                edge_col_name=edge_col_name,
                match_keys_source=match_keys_source,
                match_keys_target=match_keys_target,
            )
            db_client.execute(q_edges2)

        # TODO test that there are only two edges in mentions_mentions_edges collection

    @unittest.skip("")
    def test_insert_return(self):
        db_args = dict(self.db_args)
        db_args["database"] = "testdb"
        conn_conf = ConfigFactory.create_config(args=db_args)
        with ConnectionManager(connection_config=conn_conf) as db_client:
            cnames = [
                c["name"]
                for c in db_client.get_collections()
                if c["name"][0] != "_"
            ]

            docs = [{"value": i} for i in range(5)]
            query0 = insert_return_batch(docs, "test")

            cursor = db_client.execute(query0)
        for item in cursor:
            logger.info(item)

    @unittest.skip("")
    def test_query(self):
        db_args = dict(self.db_args)
        db_args["database"] = "testdb"
        conn_conf = ConfigFactory.create_config(args=db_args)

        q = """for doc in analysts limit 5 return doc
        """

        with ConnectionManager(connection_config=conn_conf) as db_client:
            cursor = db_client.execute(q)
        chunk = list(cursor.batch())
        logger.info(chunk)

    # @unittest.skip("")
    def test_fetch_fields(self):
        db_args = dict(self.db_args)
        db_args["database"] = "testdb"
        conn_conf = ConfigFactory.create_config(args=db_args)
        with ConnectionManager(connection_config=conn_conf) as db_client:
            docs = [{"arxiv": "current.123"}]
            map_key = fetch_fields(
                db_client, docs, "publications", ["arxiv"], ["_key"]
            )
        print(map_key)


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    unittest.main()
