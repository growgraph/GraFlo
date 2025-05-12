import logging

import pytest
import yaml

from graphcast.architecture.actor import ActionContext, ActorWrapper
from graphcast.architecture.vertex import VertexConfig

logger = logging.getLogger(__name__)


@pytest.fixture()
def vertex_config_cross():
    tc = yaml.safe_load("""
    vertex_config:
    vertices:
    -   name: person
        fields:
        -   id
        indexes:
        -   fields:
            -   id
    -   name: company
        fields:
        -   id
        indexes:
        -   fields:
            -   id
    """)
    return VertexConfig.from_dict(tc)


@pytest.fixture()
def sample_cross():
    an = yaml.safe_load("""
    -   name: John
        id: Apple
    -   name: Mary
        id: Oracle
    """)
    return an


@pytest.fixture()
def resource_cross():
    an = yaml.safe_load("""
    -   vertex: person
    -   vertex: company 
    -   target_vertex: person
        map:
            name: id
    """)
    return an


def test_wrapper_openalex(resource_cross, vertex_config_cross, sample_cross):
    ctx = ActionContext()
    anw = ActorWrapper(*resource_cross)
    anw.finish_init(transforms={}, vertex_config=vertex_config_cross)
    ctx = anw(ctx, doc=sample_cross)
    assert ctx.acc["person"] == [{"id": "John"}, {"id": "Mary"}]
    assert ctx.acc["company"] == [{"id": "Apple"}, {"id": "Oracle"}]
