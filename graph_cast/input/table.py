from __future__ import annotations

import logging
from collections import defaultdict
from functools import partial
from itertools import combinations, product

from graph_cast.architecture import ConfiguratorType
from graph_cast.architecture.general import (
    Configurator,
    DataSourceType,
    VertexConfig,
)
from graph_cast.architecture.schema import (
    EdgeType,
    TypeVE,
    _source_aux,
    _target_aux,
)
from graph_cast.architecture.table import TableConfig, Transform
from graph_cast.input.util import normalize_unit

logger = logging.getLogger(__name__)


def table_to_collections(
    rows: list[list],
    header_dict: dict[str, int],
    conf: ConfiguratorType,
) -> list[defaultdict[TypeVE, list]]:
    vertex_conf = conf.vertex_config

    rows_raw = [{k: item[v] for k, v in header_dict.items()} for item in rows]

    transform_row_partial = partial(
        transform_row,
        current_transformations=conf.current_transformations,
    )

    docs: list[defaultdict[TypeVE, list]] = list(
        map(transform_row_partial, rows_raw)
    )

    # if blank collection has no aux fields - inflate it
    for unit in docs:
        for vertex in vertex_conf.blank_collections:
            # if blank collection is in batch - add it
            if vertex not in unit:
                unit[vertex] = [{}]

    # apply filter : add a flag
    for unit in docs:
        for vertex, doc_list in unit.items():
            for doc in doc_list:
                for cfilter in conf.vertex_config.filters(vertex):
                    if not cfilter(doc):
                        doc.update(
                            {f"_status@{cfilter.b.field}": cfilter(doc)}
                        )

    docs = [normalize_unit(unit, conf) for unit in docs]

    # apply filter : add a flag
    for unit in docs:
        for u, v in conf.current_edges:
            g = u, v
            if (
                u not in vertex_conf.blank_collections
                and v not in vertex_conf.blank_collections
            ):
                if conf.graph(u, v).type == EdgeType.DIRECT:
                    ziter: product | combinations
                    if u != v:
                        ziter = product(unit[u], unit[v])
                    else:
                        ziter = combinations(unit[u], r=2)
                    for udoc, vdoc in ziter:
                        if not (
                            any(
                                [
                                    f"_status@{xkey}" in udoc
                                    for xkey in vertex_conf.fields(u)
                                ]
                            )
                            or any(
                                [
                                    f"_status@{ykey}" in vdoc
                                    for ykey in vertex_conf.fields(v)
                                ]
                            )
                        ):
                            edoc = {_source_aux: udoc, _target_aux: vdoc}
                            # add weights from available row data
                            cfields = conf.graph_config.graph(
                                u, v
                            ).weight_fields
                            # add weights from available data
                            # if cfields:
                            #     weights = [
                            #         {f: item[f] for f in cfields}
                            #         for item in rows_working
                            #     ]
                            #     ebatch = [
                            #         {**item, **attr}
                            #         for item, attr in zip(ebatch, weights)
                            #     ]
                            for vertex_weight in conf.graph_config.graph(
                                u, v
                            ).weight_vertices:
                                if vertex_weight.name == u:
                                    cbatch = udoc
                                elif vertex_weight.name == v:
                                    cbatch = vdoc
                                else:
                                    continue
                                weights = {
                                    f: cbatch[f] for f in vertex_weight.fields
                                }
                                edoc = {**edoc, **weights}
                            unit[g].append(edoc)

    # for u, vlists in vdocs.items():
    #     stub = [
    #         [
    #             item
    #             for item in vlist
    #             if not any(
    #                 [
    #                     f"_status@{xkey}" in item
    #                     for xkey in vertex_conf.fields(u)
    #                 ]
    #             )
    #         ]
    #         for vlist in vlists
    #     ]
    #     vdocs_output[u] = list(chain.from_iterable(stub))
    return docs


def transform_row(
    doc: dict, tc: TableConfig, vc: VertexConfig
) -> defaultdict[TypeVE, list]:
    docs: defaultdict[TypeVE, list] = defaultdict(list)
    # all_fields_to_transform = tc.fields()
    for vertex in tc.vertices:
        docs[vertex] += [
            tau(doc, __return_doc=True) for tau in tc.transforms(vertex)
        ]
        addendum = [
            {k: doc[k] for k in set(docs.keys()) & set(vc.fields(vertex))}
        ]
        docs[vertex] += [d for d in addendum if d]
    return docs


# for vcol, local_map in conf.current_collections:
#     current_fields = set(vertex_conf.index(vcol).fields) | set(
#         vertex_conf.fields(vcol)
#     )
#
#     default_input = current_fields & (
#             transformation_outputs
#             | set(header_dict.keys())
#             | set(local_map.output)
#     )
#
#     subrow = [(k, row[k]) for k in default_input]
#     max_len = max(len(y) for _, y in subrow)
#     common_keys = [k for k, y in subrow if 0 < len(y) != max_len]
#     subrow2 = [row for row in subrow if len(row[1]) == max_len]
#     zip2 = zip(*(y for _, y in subrow2))
#     keys = [key for key, _ in subrow2]
#     unit_list = [dict(zip(keys, dds)) for dds in zip2]
#     for dd in unit_list:
#         for c in common_keys:
#             dd[c] = row[0]
