import logging

from graph_cast.architecture.schema import Vertex

logger = logging.getLogger(__name__)


def test_init(yaml_vertex_pub):
    vc = Vertex.from_dict(yaml_vertex_pub)
    assert len(vc.indexes) == 3
    assert len(vc.fields) == 4
