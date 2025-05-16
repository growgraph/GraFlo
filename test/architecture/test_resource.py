import logging

from graphcast.architecture.resource import Resource

logger = logging.getLogger(__name__)


def test_schema_tree(schema):
    sch = schema("kg")
    mn = Resource.from_dict(sch["resources"][0])
    assert mn.count() == 14
