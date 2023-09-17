import logging

import pytest
from suthing import FileHandle

from graph_cast.architecture import TConfigurator
from graph_cast.db import ConnectionManager

from .conftest import conn_conf

logger = logging.getLogger(__name__)


@pytest.fixture
def schema():
    schema = FileHandle.load(f"test.config.schema", f"review.yaml")
    schema_obj = TConfigurator(schema)
    return schema_obj


def test_create_vertex_index(conn_conf, schema):
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.define_vertex_indices(schema.vertex_config)
    with ConnectionManager(connection_config=conn_conf) as db_client:
        q = "SHOW INDEX;"
        cursor = db_client.execute(q)
        data = cursor.data()
    assert any([item["name"] == "research_field_id" for item in data]) & any(
        [item["name"] == "author_id_full_name" for item in data]
    )


def test_create_edge_index(conn_conf, schema):
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.define_edge_indices(schema.graph_config)
    with ConnectionManager(connection_config=conn_conf) as db_client:
        q = "SHOW INDEX;"
        cursor = db_client.execute(q)
        data = cursor.data()
    assert any([item["name"] == "belongsTo_t_obs" for item in data])
