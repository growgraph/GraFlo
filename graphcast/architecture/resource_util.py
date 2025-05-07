from collections import defaultdict
from itertools import combinations, product

from graphcast.architecture.edge import Edge
from graphcast.architecture.onto import (
    SOURCE_AUX,
    TARGET_AUX,
    EdgeType,
    GraphEntity,
)
from graphcast.architecture.vertex import VertexConfig
from graphcast.util.merge import merge_doc_basis


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


def define_edges(
    unit: defaultdict[GraphEntity, list[dict]],
    unit_weights: defaultdict[GraphEntity, list[dict]],
    current_edges: list[Edge],
    vertex_conf: VertexConfig,
) -> defaultdict[GraphEntity, list[dict]]:
    for e in current_edges:
        u, v, _ = e.source, e.target, e.relation
        # blank_collections : db ids have to be retrieved to define meaningful edges
        if not (u in vertex_conf.blank_vertices or v in vertex_conf.blank_vertices):
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
                            weights = {f: cbatch[f] for f in vertex_weight.fields}
                            edoc.update(weights)
                    for ud in unit_weights[e.edge_id]:
                        edoc.update(ud)
                    unit[e.edge_id].append(edoc)
    return unit
