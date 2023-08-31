import logging

import pytest
from suthing import FileHandle

from graph_cast.db import ConfigFactory, ConnectionManager
from graph_cast.db.arango.util import insert_return_batch

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def collection_name():
    return "collection0"


@pytest.fixture(scope="function")
def test_db_name():
    return "testdb"


@pytest.fixture(scope="function")
def conn_conf():
    db_args = {
        "protocol": "http",
        "ip_addr": "localhost",
        "port": 8535,
        "cred_name": "root",
        "database": "_system",
        "db_type": "arango",
    }
    cred_pass = FileHandle.load("docker.arango", "test.arango.secret")
    db_args["cred_pass"] = cred_pass
    conn_conf = ConfigFactory.create_config(args=db_args)
    return conn_conf


@pytest.fixture
def create_db(conn_conf, test_db_name):
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.create_database(test_db_name)


@pytest.fixture
def create_collection(conn_conf, test_db_name, collection_name):
    conn_conf.database = test_db_name
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.create_collection(collection_name)


def test_create_db(create_db):
    _ = create_db


def test_create_collection(create_db, create_collection):
    _ = create_db
    _ = create_collection


def test_insert_return(
    conn_conf, create_db, create_collection, collection_name, test_db_name
):
    _ = create_db
    _ = create_collection
    conn_conf.database = test_db_name
    with ConnectionManager(connection_config=conn_conf) as db_client:
        docs = [{"value": i} for i in range(5)]
        query0 = insert_return_batch(docs, collection_name)
        cursor = db_client.execute(query0)
    for item in cursor:
        logger.info(item)
