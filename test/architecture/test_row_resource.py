import logging

from graph_cast.architecture.resource import RowResource
from graph_cast.architecture.vertex import VertexConfig

logger = logging.getLogger(__name__)


def test_init_simple(row_resource_ibes):
    rr = RowResource.from_dict(row_resource_ibes)
    assert len(rr.transforms) == 6


def test_finish(row_resource_ibes, vertex_config_ibes):
    rr = RowResource.from_dict(row_resource_ibes)
    vc = VertexConfig.from_dict(vertex_config_ibes)
    rr.finish_init(vc)
    assert rr._vertices == {
        "recommendation",
        "agency",
        "analyst",
        "ticker",
        "publication",
    }
    assert rr.fields("recommendation") == {
        "etext",
        "erec",
        "irec",
        "itext",
    }

    assert rr.fields() == {
        "cname",
        "aname",
        "cusip",
        "datetime_announce",
        "oftic",
        "initial",
        "itext",
        "datetime_review",
        "erec",
        "last_name",
        "etext",
        "irec",
    }
