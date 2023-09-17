import logging
from functools import partial
from test.transform.conftest import (
    df_ibes,
    df_transform_collision,
    edge_config_ibes,
    row_doc_ibes,
    table_config_ibes,
    table_config_transform_collision,
    tconf_ibes,
    vertex_config_ibes,
    vertex_config_transform_collision,
)

from graph_cast.architecture.schema import (
    SOURCE_AUX,
    TARGET_AUX,
    EdgeType,
    TypeVE,
    VertexConfig,
)
from graph_cast.architecture.table import TableConfig
from graph_cast.input.table import (
    add_blank_collections,
    define_edges,
    normalize_row,
    transform_row,
)
from graph_cast.util.merge import merge_doc_basis, merge_documents

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
    tr = transform_row(docs[0], conf_obj)
    assert {k: len(v) for k, v in tr.items()} == {
        "recommendation": 1,
        "analyst": 1,
        "ticker": 1,
        "publication": 2,
        "agency": 1,
    }


def test_merge_doc(vertex_config_ibes, row_doc_ibes):
    vc = VertexConfig(vertex_config_ibes)
    doc_upd = {}
    for k, item in row_doc_ibes.items():
        doc_upd[k] = merge_doc_basis(item, tuple(vc.index(k).fields))
    assert {k: len(v) for k, v in doc_upd.items()} == {
        "recommendation": 1,
        "analyst": 1,
        "ticker": 1,
        "publication": 1,
        "agency": 1,
    }


def test_transform_collision(
    table_config_transform_collision,
    vertex_config_transform_collision,
    df_transform_collision,
):
    vc = VertexConfig(vertex_config_transform_collision)
    tc = TableConfig(table_config_transform_collision, vc)
    docs = [
        dict(zip(df_transform_collision.columns, row))
        for _, row in df_transform_collision.iterrows()
    ]
    tc.add_passthrough_transformations(docs[0].keys(), vc)
    tr = transform_row(docs[0], tc)
    assert {k: len(v) for k, v in tr.items()} == {"pet": 1, "person": 1}
    assert len(tr["person"][0]) == 2


def test_derive_edges(tconf_ibes, df_ibes):
    header_dict = dict(zip(df_ibes.columns, range(df_ibes.shape[1])))
    conf = tconf_ibes
    rows = df_ibes.values.tolist()

    vertex_conf = conf.vertex_config

    rows_dressed = [
        {k: item[v] for k, v in header_dict.items()} for item in rows
    ]
    conf.set_mode("ibes")

    conf.current_transform_config.add_passthrough_transformations(
        header_dict.keys(), vertex_conf
    )

    transform_row_partial = partial(
        transform_row,
        table_config=conf.current_transform_config,
    )

    predocs_transformed = [transform_row_partial(x) for x in rows_dressed]

    docs = [normalize_row(item, vertex_conf) for item in predocs_transformed]

    docs = [add_blank_collections(item, vertex_conf) for item in docs]

    docs = [
        define_edges(
            item,
            conf.current_edges,
            vertex_conf=vertex_conf,
            graph_config=conf.graph_config,
        )
        for item in docs
    ]
    assert {k: len(v) for k, v in docs[0].items()} == {
        "recommendation": 1,
        "analyst": 1,
        "ticker": 1,
        "publication": 1,
        "agency": 1,
        ("analyst", "agency"): 1,
    }
    assert SOURCE_AUX in docs[0][("analyst", "agency")][0]
    assert TARGET_AUX in docs[0][("analyst", "agency")][0]
