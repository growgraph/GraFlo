"""Edge creation and weight management utilities for graph actors.

This module provides core functionality for creating and managing edges in the graph
database system. It handles edge rendering, weight management, and blank collection
creation. The module is central to the graph construction process, implementing the
logic for connecting vertices and managing their relationships.

Key Components:
    - add_blank_collections: Creates blank collections for vertices
    - render_edge: Core edge creation logic, handling different edge types and weights
    - render_weights: Manages edge weights and their relationships

Edge Creation Process:
    1. Edge rendering (render_edge):
       - Handles both PAIR_LIKE and PRODUCT_LIKE edge types
       - Manages source and target vertex relationships
       - Processes edge weights and relation fields
       - Creates edge documents with proper source/target mappings

    2. Weight management (render_weights):
       - Processes vertex-based weights
       - Handles direct field mappings
       - Manages weight filtering and transformation
       - Applies weights to edge documents

Example:
    >>> edge = Edge(source="user", target="post")
    >>> edges = render_edge(edge, vertex_config, acc_vertex)
    >>> edges = render_weights(edge, vertex_config, acc_vertex, cdoc, edges)
"""

from __future__ import annotations

import logging
from collections import defaultdict
from functools import partial
from itertools import combinations, product
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
    """Add blank collections for vertices that require them.

        This function creates blank collections for vertices marked as blank in the
        vertex configuration. It copies relevant fields from the current document
        to create the blank vertex documents.
    edg
        Args:
            ctx: Current action context containing document and accumulator
            vertex_conf: Vertex configuration containing blank vertex definitions

        Returns:
            ActionContext: Updated context with new blank collections

        Example:
            >>> ctx = add_blank_collections(ctx, vertex_config)
            >>> print(ctx.acc_global['blank_vertex'])
            [{'field1': 'value1', 'field2': 'value2'}]
    """
    # add blank collections
    for vname in vertex_conf.blank_vertices:
        v = vertex_conf[vname]
        prep_doc = {
            f: ctx.buffer_transforms[f] for f in v.fields if f in ctx.buffer_transforms
        }
        if vname not in ctx.acc_global:
            ctx.acc_global[vname] = [prep_doc]
    return ctx


def render_edge(
    edge: Edge,
    vertex_config: VertexConfig,
    acc_vertex: defaultdict[str, defaultdict[Optional[str], list]],
    buffer_transforms=None,
) -> defaultdict[Optional[str], list]:
    """Create edges between source and target vertices.

    This is the core edge creation function that handles different edge types
    (PAIR_LIKE and PRODUCT_LIKE) and manages edge weights. It processes source
    and target vertices, their discriminants, and creates appropriate edge
    documents with proper source/target mappings.

    Args:
        edge: Edge configuration defining the relationship
        vertex_config: Vertex configuration for source and target
        acc_vertex: Accumulated vertex documents organized by vertex name and discriminant
        buffer_transforms: Current document being processed


    Returns:
        defaultdict[Optional[str], list]: Created edges organized by relation type

    Note:
        - PAIR_LIKE edges create one-to-one relationships
        - PRODUCT_LIKE edges create cartesian product relationships
        - Edge weights are extracted from source and target vertices
        - Relation fields can be specified in either source or target
    """
    # get source and target names
    if buffer_transforms is None:
        buffer_transforms = list()
    source, target = edge.source, edge.target
    relation = None

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
        item for item in source_items if any(k in item.vertex for k in source_index)
    ]
    target_items = [
        item for item in target_items if any(k in item.vertex for k in target_index)
    ]

    if edge.casting_type == EdgeCastingType.PAIR_LIKE:
        iterator: Callable[..., Iterable[Any]] = zip
    elif edge.casting_type == EdgeCastingType.PRODUCT_LIKE:
        iterator = product
    elif edge.casting_type == EdgeCastingType.COMBINATIONS_LIKE:

        def iterator(*x):
            return partial(combinations, r=2)(x[0])

    # edges for a selected pair (source, target) but potentially different relation flavors
    edges: defaultdict[Optional[str], list] = defaultdict(list)

    for u_, v_ in iterator(source_items, target_items):
        u = u_.vertex
        v = v_.vertex
        # adding weight from source or target
        weight = dict()
        if edge.weights is not None:
            for field in edge.weights.direct:
                if field in u_.ctx:
                    weight[field] = u_.ctx[field]
                if field in v_.ctx:
                    weight[field] = v_.ctx[field]

        a = project_dict(u, source_index)
        b = project_dict(v, target_index)

        if edge.relation_field is not None:
            u_relation = u_.ctx.pop(edge.relation_field, None)
            v_relation = v_.ctx.pop(edge.relation_field, None)
            if v_relation is not None:
                a, b = b, a
                relation = v_relation
            else:
                relation = u_relation

        edges[relation] += [
            {
                **{
                    SOURCE_AUX: a,
                    TARGET_AUX: b,
                },
                **weight,
            }
        ]
    return edges


def render_weights(
    edge: Edge,
    vertex_config: VertexConfig,
    acc_vertex: defaultdict[str, defaultdict[Optional[str], list]],
    buffer_transforms: list[dict],
    edges: defaultdict[Optional[str], list],
):
    """Process and apply weights to edge documents.

    This function handles the complex weight management system, including:
    - Vertex-based weights from related vertices
    - Direct field mappings from the current document
    - Weight filtering and transformation
    - Application of weights to edge documents

    Args:
        edge: Edge configuration containing weight definitions
        vertex_config: Vertex configuration for weight processing
        acc_vertex: Accumulated vertex documents
        buffer_transforms: Current document being processed
        edges: Edge documents to apply weights to

    Returns:
        defaultdict[Optional[str], list]: Updated edge documents with applied weights

    Note:
        Weights can come from:
        1. Related vertices (vertex_classes)
        2. Direct field mappings (direct)
        3. Field transformations (map)
        4. Default index fields
    """
    vertex_weights = [] if edge.weights is None else edge.weights.vertices
    weight: dict = {}

    for w in vertex_weights:
        vertex = w.name
        if vertex is None or vertex not in vertex_config.vertex_set:
            continue
        vertex_sample = [item.vertex for item in acc_vertex[vertex][w.discriminant]]

        # find all vertices satisfying condition
        if w.filter:
            vertex_sample = [
                doc
                for doc in vertex_sample
                if all([doc[q] == v in doc for q, v in w.filter.items()])
            ]
        if vertex_sample:
            for doc in vertex_sample:
                if w.fields:
                    weight = {
                        **weight,
                        **{
                            w.cfield(field): doc[field]
                            for field in w.fields
                            if field in doc
                        },
                    }
                if w.map:
                    weight = {
                        **weight,
                        **{q: doc[k] for k, q in w.map.items()},
                    }
                if not w.fields and not w.map:
                    try:
                        weight = {
                            f"{vertex}.{k}": doc[k]
                            for k in vertex_config.index(vertex)
                            if k in doc
                        }
                    except ValueError:
                        weight = {}
                        logger.error(
                            " weights mapper error : weight definition on"
                            f" {edge.source} {edge.target} refers to"
                            f" a non existent vcollection {vertex}"
                        )
    if edge.weights is not None:
        acc = {
            k: item[k]
            for k in edge.weights.direct
            for item in buffer_transforms
            if k in item
        }
        weight.update(acc)

    if weight:
        for r, edocs in edges.items():
            edges[r] = [{**edoc, **weight} for edoc in edocs]
    return edges
