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
        -   name
        indexes:
        -   fields:
            -   name
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
    -   map:
            name: id
            id: name
    """)
    return an


@pytest.fixture()
def resource_cross_implicit():
    an = yaml.safe_load("""
    -   map:
            name: id
            id: name
    """)
    return an


def test_actor_wrapper_openalex(resource_cross, vertex_config_cross, sample_cross):
    ctx = ActionContext()
    anw = ActorWrapper(*resource_cross)
    anw.finish_init(transforms={}, vertex_config=vertex_config_cross)
    ctx = anw(ctx, doc=sample_cross)
    assert ctx.level_acc["person"] == [{"id": "John"}, {"id": "Mary"}]
    assert ctx.level_acc["company"] == [{"name": "Apple"}, {"name": "Oracle"}]


def test_actor_wrapper_openalex_implicit(
    resource_cross_implicit, vertex_config_cross, sample_cross
):
    ctx = ActionContext()
    anw = ActorWrapper(*resource_cross_implicit)
    anw.finish_init(transforms={}, vertex_config=vertex_config_cross)
    ctx = anw(ctx, doc=sample_cross)
    assert ctx.level_acc["person"] == [{"id": "John"}, {"id": "Mary"}]
    assert ctx.level_acc["company"] == [{"name": "Apple"}, {"name": "Oracle"}]
