import logging

from graphcast.architecture.actor import ActorWrapper
from graphcast.architecture.onto import ActionContext

logger = logging.getLogger(__name__)


def test_wrapper_openalex(resource_cross, vertex_config_cross, sample_cross):
    ctx = ActionContext()
    anw = ActorWrapper(*resource_cross)
    anw.finish_init(transforms={}, vertex_config=vertex_config_cross)
    ctx = anw(ctx, doc=sample_cross)
    assert ctx.acc_vertex_local["person"][None] == [{"id": "John"}, {"id": "Mary"}]
    assert ctx.acc_vertex_local["company"][None] == [{"id": "Apple"}, {"id": "Oracle"}]
