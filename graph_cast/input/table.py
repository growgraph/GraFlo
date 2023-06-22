from __future__ import annotations

import logging
from collections import ChainMap, defaultdict
from itertools import chain, combinations, product
from typing import Any

from graph_cast.architecture import ConfiguratorType
from graph_cast.architecture.schema import EdgeType, _source_aux, _target_aux
from graph_cast.architecture.transform import transform_foo

logger = logging.getLogger(__name__)


def table_to_collections(
    rows: list[list],
    header_dict: dict[str, int],
    conf: ConfiguratorType,
) -> tuple[
    defaultdict[str, list[dict[str, Any]]],
    defaultdict[tuple[str, str], list[dict[str, Any]]],
]:
    vdocs: defaultdict[str, list[list[dict[str, Any]]]] = defaultdict(list)
    edocs: defaultdict[tuple[str, str], list[dict[str, Any]]] = defaultdict(
        list
    )
    vertex_conf = conf.vertex_config

    rows_raw = [{k: item[v] for k, v in header_dict.items()} for item in rows]

    # perform possible transforms
    transformation_outputs = set(
        [
            item
            for transformation in conf.current_transformations
            for item in transformation.output
        ]
    )
    rows_working = []

    for doc in rows_raw:
        transformed = [
            transform_foo(transformation, doc)
            for transformation in conf.current_transformations
        ]
        doc_upd = {**doc, **dict(ChainMap(*transformed))}
        rows_working += [doc_upd]

    for vcol, local_map in conf.current_collections:
        vdoc_acc = []

        current_fields = set(vertex_conf.index(vcol).fields) | set(
            vertex_conf.fields(vcol)
        )

        default_input = current_fields & (
            transformation_outputs | set(header_dict.keys())
        )

        if default_input:
            vdoc_acc += [
                [{f: item[f] for f in default_input} for item in rows_working]
            ]

        if local_map.active:
            vdoc_acc += [[local_map(item) for item in rows_working]]
        vdocs[vcol].append([dict(ChainMap(*auxs)) for auxs in zip(*vdoc_acc)])

    # if blank collection has no aux fields - inflate it
    for vcol in vertex_conf.blank_collections:
        # if blank collection is in vdocs - inflate it, otherwise - create
        if vcol in vdocs:
            for j, docs in enumerate(vdocs[vcol]):
                if not docs:
                    vdocs[vcol][j] = [{}] * len(rows)
        else:
            vdocs[vcol].append([{}] * len(rows))

    # apply filter : add a flag
    for vcol, vfilter in conf.vertex_config.filters():
        for j, clist in enumerate(vdocs[vcol]):
            for doc in clist:
                if not vfilter(doc):
                    doc.update({f"_status@{vfilter.b.field}": vfilter(doc)})

    for u, v in conf.current_graphs:
        g = u, v
        if (
            u not in vertex_conf.blank_collections
            and v not in vertex_conf.blank_collections
        ):
            if conf.graph(u, v).type == EdgeType.DIRECT:
                ziter: product | combinations
                if u != v:
                    ziter = product(vdocs[u], vdocs[v])
                else:
                    ziter = combinations(vdocs[u], r=2)
                for ubatch, vbatch in ziter:
                    ebatch = [
                        {_source_aux: x, _target_aux: y}
                        for x, y in zip(ubatch, vbatch)
                        if not (
                            any(
                                [
                                    f"_status@{xkey}" in x
                                    for xkey in vertex_conf.fields(u)
                                ]
                            )
                            or any(
                                [
                                    f"_status@{ykey}" in y
                                    for ykey in vertex_conf.fields(v)
                                ]
                            )
                        )
                    ]
                    # add weights from available rows
                    cfields = conf.graph_config.graph(u, v).weight_fields
                    if cfields:
                        weights = [
                            {f: item[f] for f in cfields}
                            for item in rows_working
                        ]
                        ebatch = [
                            {**item, **attr}
                            for item, attr in zip(ebatch, weights)
                        ]
                    for vertex_weight in conf.graph_config.graph(
                        u, v
                    ).weight_vertices:
                        if vertex_weight.name == u:
                            cbatch = ubatch
                        elif vertex_weight.name == v:
                            cbatch = vbatch
                        else:
                            continue
                        weights = [
                            {f: item[f] for f in vertex_weight.fields}
                            for item in cbatch
                        ]
                        ebatch = [
                            {**item, **attr}
                            for item, attr in zip(ebatch, weights)
                        ]
                    edocs[g].extend(ebatch)

    vdocs_output: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for u, vlists in vdocs.items():
        stub = [
            [
                item
                for item in vlist
                if not any(
                    [
                        f"_status@{xkey}" in item
                        for xkey in vertex_conf.fields(u)
                    ]
                )
            ]
            for vlist in vlists
        ]
        vdocs_output[u] = list(chain.from_iterable(stub))

    return vdocs_output, edocs
