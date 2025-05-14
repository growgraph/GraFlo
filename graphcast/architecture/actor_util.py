from __future__ import annotations

import logging
from collections import defaultdict
from itertools import product
from typing import Any, Callable, Iterable, Optional

from graphcast.architecture.edge import Edge
from graphcast.architecture.onto import (
    SOURCE_AUX,
    TARGET_AUX,
    ActionContext,
    EdgeCastingType,
)
from graphcast.architecture.util import project_dict
from graphcast.architecture.vertex import VertexConfig

logger = logging.getLogger(__name__)


def add_blank_collections(
    ctx: ActionContext, vertex_conf: VertexConfig
) -> ActionContext:
    # add blank collections
    for vname in vertex_conf.blank_vertices:
        v = vertex_conf[vname]
        prep_doc = {f: ctx.cdoc[f] for f in v.fields if f in ctx.cdoc}
        if vname not in ctx.acc_global:
            ctx.acc_global[vname] = [prep_doc]
    return ctx


def render_edge(
    edge: Edge,
    vertex_config: VertexConfig,
    acc_vertex: defaultdict[str, defaultdict[Optional[str], list]],
) -> defaultdict[Optional[str], list]:
    # get source and target names
    source, target = edge.source, edge.target
    relation = edge.relation

    # get source and target edge fields
    source_index, target_index = (
        vertex_config.index(source),
        vertex_config.index(target),
    )

    # get source and target items
    source_items, target_items = (
        acc_vertex[source].get(edge.source_discriminant, []),
        acc_vertex[target].get(edge.target_discriminant, []),
    )
    source_items = [
        item for item in source_items if any(k in item for k in source_index)
    ]
    target_items = [
        item for item in target_items if any(k in item for k in target_index)
    ]

    if edge.casting_type == EdgeCastingType.PAIR_LIKE:
        iterator: Callable[..., Iterable[Any]] = zip
    else:
        iterator = product

    # edges for a selected pair (source, target) but potentially different relation flavors
    edges: defaultdict[Optional[str], list] = defaultdict(list)

    for u, v in iterator(source_items, target_items):
        # adding weight from source or target
        weight = dict()
        if edge.weights is not None:
            for field in edge.weights.source_fields:
                if field in u:
                    weight[field] = u[field]
                    if field not in edge.non_exclusive:
                        del u[field]
            for field in edge.weights.target_fields:
                if field in v:
                    weight[field] = v[field]
                    if field not in edge.non_exclusive:
                        del v[field]
        if edge.source_relation_field is not None:
            relation = u.pop(edge.source_relation_field, None)
        if edge.target_relation_field is not None:
            relation = v.pop(edge.target_relation_field, None)

        edges[relation] += [
            {
                **{
                    SOURCE_AUX: project_dict(u, source_index),
                    TARGET_AUX: project_dict(v, target_index),
                },
                **weight,
            }
        ]
    return edges


def render_weights(
    edge: Edge,
    vertex_config: VertexConfig,
    acc_vertex: defaultdict[str, defaultdict[Optional[str], list]],
    cdoc: dict,
    edges: defaultdict[Optional[str], list],
):
    vertex_classes = [] if edge.weights is None else edge.weights.vertices
    weight: dict = {}

    for vertex_weight_conf in vertex_classes:
        if vertex_weight_conf.name is None:
            continue
        vertex_sample = [
            doc
            for doc in acc_vertex[vertex_weight_conf.name][
                vertex_weight_conf.discriminant
            ]
        ]

        # find all vertices satisfying condition
        if vertex_weight_conf.filter:
            vertex_sample = [
                doc
                for doc in vertex_sample
                if all(
                    [doc[q] == v in doc for q, v in vertex_weight_conf.filter.items()]
                )
            ]
        if vertex_sample:
            doc = vertex_sample[0]
            if vertex_weight_conf.fields:
                weight = {
                    **weight,
                    **{
                        vertex_weight_conf.cfield(field): doc[field]
                        for field in vertex_weight_conf.fields
                        if field in doc
                    },
                }
            if vertex_weight_conf.map:
                weight = {
                    **weight,
                    **{q: doc[k] for k, q in vertex_weight_conf.map.items()},
                }
            if not vertex_weight_conf.fields and not vertex_weight_conf.map:
                try:
                    weight = {
                        f"{vertex_weight_conf.name}.{k}": doc[k]
                        for k in vertex_config.index(vertex_weight_conf.name)
                        if k in doc
                    }
                except ValueError:
                    weight = {}
                    logger.error(
                        " weights mapper error : weight definition on"
                        f" {edge.source} {edge.target} refers to"
                        f" a non existent vcollection {vertex_weight_conf.name}"
                    )
    if edge.weights is not None:
        weight = {
            **weight,
            **{k: cdoc[k] for k in edge.weights.direct if k in cdoc},
        }

    if weight:
        for r, edocs in edges.items():
            edges[r] = [{**edoc, **weight} for edoc in edocs]
    return edges
