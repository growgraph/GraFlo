import logging
from test.arangos.conftest import conn_conf, create_db, test_db_name

import pytest

from graph_cast.db import ConnectionManager
from graph_cast.db.arango.util import insert_return_batch

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def collection_name():
    return "collection0"


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
