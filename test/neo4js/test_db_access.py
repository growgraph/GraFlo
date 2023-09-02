import logging

from graph_cast.db import ConnectionManager

from .conftest import conn_conf

logger = logging.getLogger(__name__)


def test_db_access(conn_conf):
    with ConnectionManager(connection_config=conn_conf) as db_client:
        query = "MATCH(n) RETURN n LIMIT 3"
        result = db_client.execute(query, params=None)


# def test_create_index(conn_conf):
#     with ConnectionManager(connection_config=conn_conf) as db_client:
#         db_client.
#         query = "MATCH(n) RETURN n LIMIT 3"
#         result = db_client.execute(query, params=None)
