import logging
from collections import defaultdict

from graphcast.architecture.onto import GraphEntity
from graphcast.architecture.resource import Resource
from graphcast.architecture.wrapper import ActorWrapper

logger = logging.getLogger(__name__)


# def test_mapper_wc(mapper_node_edge_weight_config):
#     mn = ActionNodeWrapper.from_dict(mapper_node_edge_weight_config)
#     assert len(mn.edge.weights.vertices[0].fields) == 1


def test_schema_mapper_node(schema):
    sch = schema("kg_v3b")
    mn = ActorWrapper.from_dict(sch["resources"]["tree_likes"][0]["root"])
    assert len(mn._children) == 5
    assert mn._children[-1].edge is not None
    assert mn._children[-1].edge.source == "publication"


def test_schema_tree(schema):
    sch = schema("kg_v3b")
    mn = Resource.from_dict(sch["resources"]["tree_likes"][0])
    assert len(mn.root._children) == 5


def test_mapper_value(mapper_value):
    mn = ActorWrapper.from_dict(mapper_value)
    mn.finish_init(None, None, None, None)
    test_doc = {"wikidata": "https://www.wikidata.org/wiki/Q123", "mag": 105794591}
    acc: defaultdict[GraphEntity, list] = defaultdict(list)
    acc = mn._children[0].apply(test_doc, None, acc)
    assert acc["concept"][0] == {"mag": 105794591}
    acc = mn._children[1].apply(test_doc, None, acc)
    assert acc["concept"][1] == {"wikidata": "Q123"}
