import logging

from graphcast.architecture.action_node import SimpleResource
from graphcast.architecture.schema import Schema

logger = logging.getLogger(__name__)


def test_init_simple(vertex_config_kg, edge_config_kg):
    schema = {
        "vertex_config": vertex_config_kg,
        "edge_config": edge_config_kg,
        "resources": {},
        "general": {"name": "abc"},
    }
    sch = Schema.from_dict(schema)
    assert len(sch.vertex_config.vertices) == 3
    assert len(sch.edge_config.edges) == 3


def test_schema_load(schema):
    sch = schema("kg_v3b")
    schema_obj = Schema.from_dict(sch)
    assert len(schema_obj.resources.tree_likes) == 2


def test_schema_update(schema):
    sd = schema("ibes_upd")
    sr = SimpleResource.from_dict(sd["resources"][0])
    assert len(sr.root.action_node.descendants) == 10
