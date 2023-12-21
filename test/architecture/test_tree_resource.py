import logging
from test.conftest import schema

from graph_cast.architecture.mapper import MapperNode
from graph_cast.architecture.resource import TreeResource
from graph_cast.architecture.schema import Schema

logger = logging.getLogger(__name__)


def test_mn(mapper_node_a):
    mn = MapperNode.from_dict(mapper_node_a)
    assert len(mn.transforms) == 1


def test_mapper_edge(mapper_node_edge):
    mn = MapperNode.from_dict(mapper_node_edge)
    assert mn.edge.source == "mention"


def test_mapper_tree(mapper_node_tree):
    mn = MapperNode.from_dict(mapper_node_tree)
    assert len(mn._children[0]._children) == 3


def test_mapper_wc(mapper_node_edge_weight_config):
    mn = MapperNode.from_dict(mapper_node_edge_weight_config)
    assert len(mn.edge.weights.vertices[0].fields) == 1


def test_schema_mapper_node(schema):
    sch = schema("kg_v3b")
    mn = MapperNode.from_dict(sch["resources"]["tree_likes"][0]["root"])
    assert len(mn._children) == 5


def test_schema_tree(schema):
    sch = schema("kg_v3b")
    mn = TreeResource.from_dict(sch["resources"]["tree_likes"][0])
    assert len(mn.root._children) == 5
