import logging

from graphcast.architecture.actor import ActorWrapper
from graphcast.architecture.onto import ActionContext, LocationIndex, VertexRep

logger = logging.getLogger(__name__)


def test_actor_wrapper_openalex(resource_cross, vertex_config_cross, sample_cross):
    ctx = ActionContext()
    anw = ActorWrapper(*resource_cross)
    anw.finish_init(transforms={}, vertex_config=vertex_config_cross)
    ctx = anw(ctx, doc=sample_cross)
    assert ctx.acc_vertex_local["person"][LocationIndex(path=(None,))] == [
        VertexRep(vertex={"id": "John"}, ctx={"name": "John", "id": "Apple"}),
        VertexRep(vertex={"id": "Mary"}, ctx={"name": "Mary", "id": "Oracle"}),
    ]
    assert ctx.acc_vertex_local["company"][LocationIndex(path=(None,))] == [
        VertexRep(vertex={"name": "Apple"}, ctx={"name": "John", "id": "Apple"}),
        VertexRep(vertex={"name": "Oracle"}, ctx={"name": "Mary", "id": "Oracle"}),
    ]


def test_actor_wrapper_openalex_implicit(
    resource_cross_implicit, vertex_config_cross, sample_cross
):
    ctx = ActionContext()
    anw = ActorWrapper(*resource_cross_implicit)
    anw.finish_init(transforms={}, vertex_config=vertex_config_cross)
    ctx = anw(ctx, doc=sample_cross)
    assert ctx.acc_vertex_local["person"][LocationIndex(path=(None,))] == [
        VertexRep(vertex={"id": "John"}, ctx={"name": "John", "id": "Apple"}),
        VertexRep(vertex={"id": "Mary"}, ctx={"name": "Mary", "id": "Oracle"}),
    ]
    assert ctx.acc_vertex_local["company"][LocationIndex(path=(None,))] == [
        VertexRep(vertex={"name": "Apple"}, ctx={"name": "John", "id": "Apple"}),
        VertexRep(vertex={"name": "Oracle"}, ctx={"name": "Mary", "id": "Oracle"}),
    ]
