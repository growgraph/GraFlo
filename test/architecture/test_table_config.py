import logging
from functools import partial
from test.conftest import schema
from test.transform.conftest import (
    df_ibes,
    df_ticker,
    df_transform_collision,
    edge_config_ibes,
    edge_config_ticker,
    row_doc_ibes,
    table_config_ibes,
    table_config_ticker,
    table_config_transform_collision,
    tconf_ibes,
    vertex_config_ibes,
    vertex_config_ticker,
    vertex_config_transform_collision,
)

import pytest

from graph_cast.architecture.graph import GraphConfig
from graph_cast.architecture.onto import SOURCE_AUX, TARGET_AUX
from graph_cast.architecture.schema import RowResource
from graph_cast.architecture.table import TableConfig
from graph_cast.architecture.vertex import VertexConfig
from graph_cast.flow.row import (
    add_blank_collections,
    define_edges,
    extract_weights,
    normalize_row,
    row_to_vertices,
)
from graph_cast.util.merge import merge_doc_basis, merge_documents

logger = logging.getLogger(__name__)


def test_transform_row(schema, vertex_config_ibes, df_ibes):
    sch = schema("ibes")
    vc = VertexConfig.from_dict(sch["vertex_config"])
    rr = RowResource.from_dict(sch["resources"]["rows"][0])
    docs = [dict(zip(df_ibes.columns, row)) for _, row in df_ibes.iterrows()]
    tr = row_to_vertices(docs[0], vc, rr)

    assert {k: len(v) for k, v in tr.items()} == {
        "recommendation": 1,
        "analyst": 1,
        "ticker": 1,
        "publication": 2,
        "agency": 1,
    }


@pytest.mark.skip(reason="obsolete")
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


@pytest.mark.skip(reason="obsolete")
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
    tc.add_passthrough_transformations(list(docs[0].keys()), vc)
    tr = transform_row(docs[0], tc)
    assert {k: len(v) for k, v in tr.items()} == {"pet": 1, "person": 1}
    assert len(tr["person"][0]) == 2


@pytest.mark.skip(reason="obsolete")
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
            unit,
            unit_weight={},
            current_edges=conf.current_edges,
            vertex_conf=vertex_conf,
            graph_config=conf.graph_config,
        )
        for unit in docs
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


@pytest.mark.skip(reason="obsolete")
def test_transform_row_pure_weight(
    table_config_ticker, vertex_config_ticker, edge_config_ticker, df_ticker
):
    vc = VertexConfig(vertex_config_ticker)
    gc = GraphConfig(edge_config_ticker, vc)
    tc = TableConfig(table_config_ticker, vc, gc)
    docs = [
        dict(zip(df_ticker.columns, row)) for _, row in df_ticker.iterrows()
    ]

    pure_weights = [extract_weights(unit, table_config=tc) for unit in docs]

    assert pure_weights[0] == {"t_obs": "2014-04-15T12:00:00Z"}
