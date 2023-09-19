import logging
from test.transform.conftest import edge_config_ticker, vertex_config_ticker

from graph_cast.architecture.graph import GraphConfig
from graph_cast.architecture.schema import VertexConfig

logger = logging.getLogger(__name__)


def test_transform_row_with_non_vertex(
    vertex_config_ticker, edge_config_ticker
):
    vc = VertexConfig(vertex_config_ticker)
    gc = GraphConfig(edge_config_ticker, vc)
    gc.weight_raw_fields()

    assert gc.weight_raw_fields() == {"t_obs"}
