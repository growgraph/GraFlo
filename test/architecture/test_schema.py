import logging

from graphcast.architecture.resource import Resource
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
    assert len(list(sch.edge_config.edges_items())) == 3


def test_schema_load(schema):
    sch = schema("kg")
    schema_obj = Schema.from_dict(sch)
    assert len(schema_obj.resources) == 2


def test_resource(schema):
    sd = schema("ibes")
    sr = Resource.from_dict(sd["resources"][0])
    assert len(sr.root.actor.descendants) == 10


def test_s(schema):
    sd = schema("ibes")
    sr = Schema.from_dict(sd)
    assert sr.general.name == "ibes"
