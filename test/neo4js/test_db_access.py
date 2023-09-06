import logging

import pytest
from suthing import FileHandle

from graph_cast.architecture import TConfigurator
from graph_cast.db import ConnectionManager

from .conftest import conn_conf

logger = logging.getLogger(__name__)


@pytest.fixture
def schema():
    schema = FileHandle.load(f"test.schema", f"review.yaml")
    schema_obj = TConfigurator(schema)
    return schema_obj


# @pytest.skip
# def test_init_db(conn_conf, schema: TConfigurator):
#     with ConnectionManager(connection_config=conn_conf) as db_client:
#         db_client.init_db(schema)
#     assert 1 == 1
#
#
# @pytest.skip
# def test_db_access(conn_conf):
#     with ConnectionManager(connection_config=conn_conf) as db_client:
#         query = "MATCH(n) RETURN n LIMIT 3"
#         result = db_client.execute(query, params=None)


def test_create_index(conn_conf, schema):
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.define_vertex_indices(schema.vertex_config)
    assert True
