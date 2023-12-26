import logging
from functools import partial
from test.conftest import schema
from test.transform.conftest import (
    df_ibes,
    df_ticker,
    df_transform_collision,
    row_doc_ibes,
    row_resource_transform_collision,
    vertex_config_transform_collision,
)

from graph_cast.architecture.edge import EdgeConfig
from graph_cast.architecture.onto import SOURCE_AUX, TARGET_AUX
from graph_cast.architecture.resource import (
    RowResource,
    add_blank_collections,
    define_edges,
    extract_weights,
    normalize_row,
    row_to_vertices,
)
from graph_cast.architecture.schema import Schema
from graph_cast.architecture.vertex import VertexConfig

logger = logging.getLogger(__name__)


def test_transform_row(schema, df_ibes):
    sch = schema("ibes")
    vc = VertexConfig.from_dict(sch["vertex_config"])
    ec = EdgeConfig.from_dict(sch["edge_config"])
    rr = RowResource.from_dict(sch["resources"]["row_likes"][0])
    rr.finish_init(vc, ec)
    rr.add_trivial_transformations(vc, df_ibes.columns)
    docs = [dict(zip(df_ibes.columns, row)) for _, row in df_ibes.iterrows()]
    tr = row_to_vertices(docs[0], vc, rr)

    assert {k: len(v) for k, v in tr.items()} == {
        "recommendation": 1,
        "analyst": 1,
        "ticker": 1,
        "publication": 2,
        "agency": 1,
    }


def test_merge_doc(schema, row_doc_ibes):
    sch = schema("ibes")
    vc = VertexConfig.from_dict(sch["vertex_config"])

    doc_upd = normalize_row(row_doc_ibes, vc)
    assert {k: len(v) for k, v in doc_upd.items()} == {
        "recommendation": 1,
        "analyst": 1,
        "ticker": 1,
        "publication": 1,
        "agency": 1,
    }


def test_transform_collision(
    vertex_config_transform_collision,
    row_resource_transform_collision,
    df_transform_collision,
):
    vc = VertexConfig.from_dict(vertex_config_transform_collision)
    rr = RowResource.from_dict(row_resource_transform_collision)
    ec = EdgeConfig.from_dict({})

    rr.finish_init(vc, edge_config=ec)
    rr.add_trivial_transformations(vc, df_transform_collision.columns)
    docs = [
        dict(zip(df_transform_collision.columns, row))
        for _, row in df_transform_collision.iterrows()
    ]

    tr = row_to_vertices(docs[0], vc, rr)
    assert {k: len(v) for k, v in tr.items()} == {"pet": 1, "person": 1}
    assert len(tr["person"][0]) == 2


def test_derive_edges(schema, df_ibes):
    schema_dict = schema("ibes")
    sch = Schema.from_dict(schema_dict)
    header_dict = dict(zip(df_ibes.columns, range(df_ibes.shape[1])))
    rows = df_ibes.values.tolist()

    vc = sch.vertex_config

    rows_dressed = [
        {k: item[v] for k, v in header_dict.items()} for item in rows
    ]

    rr = sch.fetch_resource("ibes")
    rr.add_trivial_transformations(vc, df_ibes.columns)

    transform_row_partial = partial(row_to_vertices, vc=vc, rr=rr)

    predocs_transformed = [transform_row_partial(x) for x in rows_dressed]

    docs = [normalize_row(item, vc) for item in predocs_transformed]

    docs = [add_blank_collections(item, vc) for item in docs]

    weights = [
        extract_weights(unit, row_resource=rr, edges=sch.edge_config.edges)
        for unit in rows_dressed
    ]

    docs = [
        define_edges(
            unit,
            unit_weights=unit_weight,
            current_edges=sch.edge_config.edges,
            vertex_conf=vc,
        )
        for unit, unit_weight in zip(docs, weights)
    ]

    assert {k: len(v) for k, v in docs[0].items()} == {
        "recommendation": 1,
        "analyst": 1,
        "ticker": 1,
        "publication": 1,
        "agency": 1,
        ("analyst", "agency", None): 1,
    }
    assert SOURCE_AUX in docs[0][("analyst", "agency", None)][0]
    assert TARGET_AUX in docs[0][("analyst", "agency", None)][0]


def test_transform_row_pure_weight(schema, df_ticker):
    schema_dict = schema("ticker")
    sch = Schema.from_dict(schema_dict)
    header_dict = dict(zip(df_ticker.columns, range(df_ticker.shape[1])))
    rows = df_ticker.values.tolist()

    vc = sch.vertex_config

    rows_dressed = [
        {k: item[v] for k, v in header_dict.items()} for item in rows
    ]
    rr = sch.fetch_resource(sch.resources.row_likes[0].name)
    rr.add_trivial_transformations(vc, df_ticker.columns)

    pure_weights = [
        extract_weights(unit, row_resource=rr, edges=sch.edge_config.edges)
        for unit in rows_dressed
    ]

    assert pure_weights[0]["ticker", "feature", None][0] == {
        "t_obs": "2014-04-15T12:00:00Z"
    }
