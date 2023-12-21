from collections import defaultdict
from itertools import combinations, product

from graph_cast.architecture.edge import Edge, EdgeConfig
from graph_cast.architecture.onto import (
    SOURCE_AUX,
    TARGET_AUX,
    EdgeType,
    GraphEntity,
)
from graph_cast.architecture.resource import RowResource
from graph_cast.architecture.vertex import VertexConfig
from graph_cast.util.merge import merge_doc_basis


def row_to_vertices(
    doc: dict, vc: VertexConfig, rr: RowResource
) -> defaultdict[GraphEntity, list]:
    """

        doc gets transformed and mapped onto vertices

    :param doc: {k: v}
    :param vc:
    :param rr:
    :return: { vertex: [doc]}
    """

    docs: defaultdict[GraphEntity, list] = defaultdict(list)
    for vertex in vc.vertices:
        docs[vertex.name] += [
            tau(doc, __return_doc=True)
            for tau in rr.fetch_transforms(vertex.name)
        ]
    return docs


def table_to_collections(
    rows: list[list],
    header_dict: dict[str, int],
    vc: VertexConfig,
    ec: EdgeConfig,
    rr: RowResource,
) -> list[defaultdict[GraphEntity, list]]:
    rows_dressed = [
        {k: item[v] for k, v in header_dict.items()} for item in rows
    ]

    # adding `trivial` transformations : when column is cast directly to a vertex field
    # NB TODO should be list of header
    rr.add_trivial_transformations(vc, list(header_dict.keys()))

    predocs_transformed = [row_to_vertices(x, vc, rr) for x in rows_dressed]

    docs = [normalize_row(item, vc) for item in predocs_transformed]

    docs = [add_blank_collections(item, vc) for item in docs]

    docs = [
        apply_filter(
            item,
            vertex_conf=vc,
        )
        for item in docs
    ]

    pure_weights = [
        extract_weights(unit, rr, ec.edges) for unit in rows_dressed
    ]

    docs = [
        define_edges(
            unit,
            unit_weights,
            ec.edges,
            vertex_conf=vc,
        )
        for unit, unit_weights in zip(docs, pure_weights)
    ]
    return docs


def normalize_row(unit, vc: VertexConfig) -> defaultdict[GraphEntity, list]:
    doc_upd: defaultdict[GraphEntity, list] = defaultdict(list)
    for k, item in unit.items():
        doc_upd[k] = merge_doc_basis(item, tuple(vc.index(k).fields))
    return doc_upd


def add_blank_collections(
    unit: defaultdict[GraphEntity, list[dict]], vertex_conf: VertexConfig
) -> defaultdict[GraphEntity, list[dict]]:
    # add blank collections
    for vertex in vertex_conf.blank_vertices:
        # if blank collection is in batch - add it
        if vertex not in unit:
            unit[vertex] = [{}]
    return unit


def apply_filter(
    unit: defaultdict[GraphEntity, list[dict]], vertex_conf: VertexConfig
) -> defaultdict[GraphEntity, list[dict]]:
    for vertex, doc_list in unit.items():
        if vertex_conf.filters(vertex):
            unit[vertex] = [
                doc
                for doc in doc_list
                if all(cfilter(doc) for cfilter in vertex_conf.filters(vertex))
            ]
    return unit


def extract_weights(
    doc: dict, row_resource: RowResource, edges: list[Edge]
) -> defaultdict[GraphEntity, list]:
    doc_upd: defaultdict[GraphEntity, list] = defaultdict(list)
    for e in edges:
        for tau in row_resource.fetch_transforms(e.edge_id):
            doc_upd[e.edge_id] += [tau(doc, __return_doc=True)]
    return doc_upd


def define_edges(
    unit: defaultdict[GraphEntity, list[dict]],
    unit_weights: defaultdict[GraphEntity, list[dict]],
    current_edges: list[Edge],
    vertex_conf: VertexConfig,
) -> defaultdict[GraphEntity, list[dict]]:
    for e in current_edges:
        u, v, r = e.source, e.target, e.relation
        # blank_collections : db ids have to be retrieved to define meaningful edges
        if not (
            u in vertex_conf.blank_vertices or v in vertex_conf.blank_vertices
        ):
            if e.type == EdgeType.DIRECT:
                ziter: product | combinations
                if u != v:
                    ziter = product(unit[u], unit[v])
                else:
                    ziter = combinations(unit[u], r=2)

                for udoc, vdoc in ziter:
                    edoc = {SOURCE_AUX: udoc, TARGET_AUX: vdoc}
                    if e.weights is not None:
                        # weights_direct = {
                        #     f: cbatch[f] for f in e.weights.direct
                        # }

                        for vertex_weight in e.weights.vertices:
                            if vertex_weight.name == u:
                                cbatch = udoc
                            elif vertex_weight.name == v:
                                cbatch = vdoc
                            else:
                                continue
                            weights = {
                                f: cbatch[f] for f in vertex_weight.fields
                            }
                            edoc.update(weights)
                    for ud in unit_weights[e.edge_id]:
                        edoc.update(ud)
                    unit[e.edge_id].append(edoc)
    return unit
