import logging

from graph_cast.architecture.vertex import Vertex

logger = logging.getLogger(__name__)


def test_init(vertex_pub):
    vc = Vertex.from_dict(vertex_pub)
    assert len(vc.indexes) == 3
    assert len(vc.fields) == 4
