import logging
from test.transform.conftest import (
    df_ibes,
    table_config_ibes,
    vertex_config_ibes,
)

from suthing import FileHandle

from graph_cast.architecture import JConfigurator, TConfigurator
from graph_cast.architecture.schema import VertexConfig
from graph_cast.architecture.table import TableConfig
from graph_cast.input.table import transform_row

logger = logging.getLogger(__name__)


def test_table_config_vertices(table_config_ibes, vertex_config_ibes):
    vc = VertexConfig(vertex_config_ibes)
    conf_obj = TableConfig(table_config_ibes, vc)
    assert {
        "recommendation",
        "agency",
        "analyst",
        "ticker",
        "publication",
    } == conf_obj.vertices


def test_table_config_fields(table_config_ibes, vertex_config_ibes):
    vc = VertexConfig(vertex_config_ibes)
    conf_obj = TableConfig(table_config_ibes, vc)
    assert conf_obj.fields("recommendation") == {
        "etext",
        "erec",
        "irec",
        "itext",
    }


def test_table_config_fields_all(table_config_ibes, vertex_config_ibes):
    vc = VertexConfig(vertex_config_ibes)
    conf_obj = TableConfig(table_config_ibes, vc)
    assert conf_obj.fields() == {
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


def test_transform_row(table_config_ibes, vertex_config_ibes, df_ibes):
    vc = VertexConfig(vertex_config_ibes)
    conf_obj = TableConfig(table_config_ibes, vc)
    docs = [dict(zip(df_ibes.columns, row)) for _, row in df_ibes.iterrows()]
    tr = transform_row(docs[0], conf_obj, vc)
    assert {k: len(v) for k, v in tr.items()} == {
        "recommendation": 1,
        "analyst": 1,
        "ticker": 1,
        "publication": 2,
        "agency": 1,
    }
