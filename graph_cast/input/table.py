from __future__ import annotations

import logging
from collections import defaultdict
from itertools import combinations, product

from graph_cast.architecture import ConfiguratorType
from graph_cast.architecture.graph import GraphConfig
from graph_cast.architecture.onto import (
    SOURCE_AUX,
    TARGET_AUX,
    EdgeType,
    TypeVE,
)
from graph_cast.architecture.schema import VertexConfig
from graph_cast.architecture.table import TableConfig
from graph_cast.util.merge import merge_doc_basis

logger = logging.getLogger(__name__)


def table_to_collections(
    rows: list[list],
    header_dict: dict[str, int],
    conf: ConfiguratorType,
) -> list[defaultdict[TypeVE, list]]:
    vertex_conf = conf.vertex_config

    rows_dressed = [
        {k: item[v] for k, v in header_dict.items()} for item in rows
    ]

    # adding `trivial` transformations : when column is cast directly to a vertex field
    conf.current_transform_config.add_passthrough_transformations(
        header_dict.keys(), vertex_conf
    )

    predocs_transformed = [
        transform_row(x, table_config=conf.current_transform_config)
        for x in rows_dressed
    ]

    pure_weights = [
        extract_weights(unit, table_config=conf.current_transform_config)
        for unit in rows_dressed
    ]

    docs = [normalize_row(item, vertex_conf) for item in predocs_transformed]

    docs = [add_blank_collections(item, vertex_conf) for item in docs]

    docs = [
        apply_filter(
            item,
            vertex_conf=vertex_conf,
        )
        for item in docs
    ]

    docs = [
        define_edges(
            unit,
            unit_weight,
            conf.current_edges,
            vertex_conf=vertex_conf,
            graph_config=conf.graph_config,
        )
        for unit, unit_weight in zip(docs, pure_weights)
    ]
    return docs


def transform_row(
    doc: dict, table_config: TableConfig
) -> defaultdict[TypeVE, list]:
    """

        doc gets transformed and mapped onto vertices

    :param doc: {k: v}
    :param table_config:
    :return: { vertex: [vertex_subdoc]}
    """

    docs: defaultdict[TypeVE, list] = defaultdict(list)
    for vertex in table_config.vertices:
        docs[vertex] += [
            tau(doc, __return_doc=True)
            for tau in table_config.transforms(vertex)
        ]
    return docs


def extract_weights(doc: dict, table_config: TableConfig) -> dict:
    doc_upd = {}
    for tau in table_config.transforms(TableConfig.RESERVED_TAU_WEIGHTS):
        doc_upd.update(tau(doc, __return_doc=True))
    return doc_upd


def normalize_row(unit, vc: VertexConfig) -> defaultdict[TypeVE, list]:
    doc_upd: defaultdict[TypeVE, list] = defaultdict(list)
    for k, item in unit.items():
        doc_upd[k] = merge_doc_basis(item, tuple(vc.index(k).fields))
    return doc_upd


def add_blank_collections(
    unit: defaultdict[TypeVE, list[dict]], vertex_conf: VertexConfig
) -> defaultdict[TypeVE, list[dict]]:
    # add blank collections
    for vertex in vertex_conf.blank_collections:
        # if blank collection is in batch - add it
        if vertex not in unit:
            unit[vertex] = [{}]
    return unit


def apply_filter(
    unit: defaultdict[TypeVE, list[dict]], vertex_conf: VertexConfig
) -> defaultdict[TypeVE, list[dict]]:
    for vertex, doc_list in unit.items():
        if vertex_conf.filters(vertex):
            unit[vertex] = [
                doc
                for doc in doc_list
                if all(cfilter(doc) for cfilter in vertex_conf.filters(vertex))
            ]
    return unit


def define_edges(
    unit: defaultdict[TypeVE, list[dict]],
    unit_weight: dict,
    current_edges,
    vertex_conf: VertexConfig,
    graph_config: GraphConfig,
) -> defaultdict[TypeVE, list[dict]]:
    for u, v in current_edges:
        g = u, v
        # blank_collections : db ids have to be retrieved to define meaningful edges
        if (
            u not in vertex_conf.blank_collections
            and v not in vertex_conf.blank_collections
        ):
            if graph_config.graph(u, v).type == EdgeType.DIRECT:
                ziter: product | combinations
                if u != v:
                    ziter = product(unit[u], unit[v])
                else:
                    ziter = combinations(unit[u], r=2)

                for udoc, vdoc in ziter:
                    edoc = {SOURCE_AUX: udoc, TARGET_AUX: vdoc}
                    for vertex_weight in graph_config.graph(
                        u, v
                    ).weight_vertices:
                        if vertex_weight.name == u:
                            cbatch = udoc
                        elif vertex_weight.name == v:
                            cbatch = vdoc
                        else:
                            continue
                        weights = {
                            f.name: cbatch[f.name]
                            for f in vertex_weight.fields
                        }
                        edoc.update(weights)
                    edoc.update(
                        {
                            q: w
                            for q, w in unit_weight.items()
                            if q in graph_config.graph(u, v).weight_fields
                        }
                    )
                    unit[g].append(edoc)
    return unit
