from __future__ import annotations

import dataclasses
import logging
from collections import defaultdict
from itertools import product

from graph_cast.architecture.edge import Edge
from graph_cast.architecture.onto import SOURCE_AUX, TARGET_AUX, GraphEntity
from graph_cast.architecture.transform import Transform
from graph_cast.architecture.util import project_dict
from graph_cast.architecture.vertex import VertexConfig
from graph_cast.onto import BaseDataclass, BaseEnum

logger = logging.getLogger(__name__)


class NodeType(str, BaseEnum):
    # only refers to other nodes
    TRIVIAL = "trivial"
    # adds a vertex specified by a value; terminal
    VALUE = "value"
    # adds a vertex, directly or via a mapping; terminal
    VERTEX = "vertex"
    # maps children nodes to a list
    LIST = "list"
    # descends, maps children nodes to the doc below
    DESCEND = "descend"
    # adds edges between collection that have already been added
    EDGE = "edge"
    # adds weights to existing edges
    WEIGHT = "weight"

    def __lt__(self, other):
        if self == other:
            return False
        for elem in NodeType:
            if self == elem:
                return True
            elif other == elem:
                return False
        raise RuntimeError("__lt__ broken")

    def __gt__(self, other):
        return not (self < other)


def discriminate(items, indices, discriminant_value, discriminant_key):
    """

    :param items: list of documents (dict)
    :param indices:
    :param discriminant_value:
    :param discriminant_key:
    :return: items
    """

    # pick items that have any of index field present
    _items = [item for item in items if any([k in item for k in indices])]

    if discriminant_value is not None:
        _items = [
            item
            for item in _items
            if discriminant_key in item
            and item[discriminant_key] == discriminant_value
        ]
    return _items


@dataclasses.dataclass
class MapperNode(BaseDataclass):
    type: NodeType = NodeType.TRIVIAL
    edge: Edge = None
    name: str | None = None
    transforms: list[Transform] = dataclasses.field(default_factory=list)
    key: str | None = None
    filter: dict = dataclasses.field(default_factory=dict)
    unfilter: dict = dataclasses.field(default_factory=dict)
    map: dict = dataclasses.field(default_factory=dict)
    discriminant: str | None = None
    children: list[dict] = dataclasses.field(default_factory=list)

    def __post_init__(self):
        self._children: list[MapperNode] = [
            MapperNode.from_dict(oo) for oo in self.children
        ]
        self._children = sorted(self._children, key=lambda x: x.type)

    def finish_init(self, vc: VertexConfig):
        assert self.edge is not None
        self.edge.finish_init(vc)
        for c in self._children:
            c.finish_init(vc)

    def passes(self, doc):
        """
        Args:
            doc:

        Returns:
            no filters - doc passes
            filter set, no unfilter - doc passes if filter is true
            no filter, unfilter set - doc passes if unfilter is true
            filter set, unfilter set - doc passes if filter and unfilter is true

        """
        if not (self.filter or self.unfilter):
            return True
        elif self.filter:
            flag = all([doc[k] == v for k, v in self.filter.items()])
            if not self.unfilter:
                return flag
            else:
                return flag and any(
                    [doc[k] != v for k, v in self.unfilter.items()]
                )
        else:
            return any([doc[k] != v for k, v in self.unfilter.items()])

    def apply(
        self,
        doc,
        vertex_config: VertexConfig,
        acc: defaultdict[GraphEntity, list],
        discriminant_key,
    ):
        if self.type == NodeType.TRIVIAL:
            for c in self._children:
                c.apply(doc, vertex_config, acc, discriminant_key)
        elif self.type == NodeType.DESCEND:
            if isinstance(doc, dict) and self.key in doc:
                sub_doc = doc[self.key]
                for c in self._children:
                    c.apply(sub_doc, vertex_config, acc, discriminant_key)
        elif self.type == NodeType.LIST:
            if not isinstance(doc, list):
                raise TypeError(f"at {self} doc is not list")
            for c in self._children:
                for sub_doc in doc:
                    c.apply(sub_doc, vertex_config, acc, discriminant_key)
        elif self.type == NodeType.VALUE:
            if self.name is not None:
                acc[self.name] += [{self.key: doc}]
        elif self.type == NodeType.VERTEX:
            if not isinstance(doc, dict):
                raise TypeError(f"at {self} doc is not dict")
            self._apply_map(doc, vertex_config, acc, discriminant_key)
        elif self.type == NodeType.EDGE:
            self._add_edges(vertex_config, acc, discriminant_key)
        elif self.type == NodeType.WEIGHT:
            self._add_weights(vertex_config, acc)
        else:
            pass

    def __repr__(self):
        s = f"(type = {self.type}, "
        s += f"descend = {self.key}"
        s += f")"
        return s

    def _apply_map(
        self,
        doc,
        vertex_config: VertexConfig,
        acc: defaultdict[GraphEntity, list],
        discriminant_key,
    ):
        if self.passes(doc) and self.name is not None:
            keys = vertex_config.fields(self.name)
            _doc = dict()
            for t in self.transforms:
                _doc.update(t(doc, __return_doc=True))

            keys += [k for k in self.map if k not in keys]

            if isinstance(doc, dict):
                _doc.update({kk: doc[kk] for kk in keys if kk in doc})
            if self.map:
                _doc = {
                    self.map[k] if k in self.map else k: v
                    for k, v in _doc.items()
                }
            if self.discriminant is not None:
                _doc.update({discriminant_key: self.discriminant})
            acc[self.name] += [_doc]

    def _add_edges(self, vertex_config: VertexConfig, acc, discriminant_key):
        assert self.edge is not None
        # get source and target names
        source, target = self.edge.source, self.edge.target

        # get source and target edge fields
        source_index, target_index = (
            vertex_config.index(source),
            vertex_config.index(target),
        )

        # get source and target items
        source_items, target_items = acc[source], acc[target]

        source_items = discriminate(
            source_items,
            source_index,
            self.edge.source_discriminant,
            discriminant_key,
        )
        target_items = discriminate(
            target_items,
            target_index,
            self.edge.target_discriminant,
            discriminant_key,
        )

        for u, v in product(source_items, target_items):
            # adding weight from source or target
            weight = dict()
            for field in self.edge.weights.source_fields:
                if field in u:
                    weight[field] = u[field]
                    if field not in self.edge.non_exclusive:
                        del u[field]
            for field in self.edge.weights.target_fields:
                if field in v:
                    weight[field] = v[field]
                    if field not in self.edge.non_exclusive:
                        del v[field]
            acc[source, target, self.edge.relation] += [
                {
                    **{
                        SOURCE_AUX: project_dict(u, source_index),
                        TARGET_AUX: project_dict(v, target_index),
                    },
                    **weight,
                }
            ]

    def _add_weights(self, vertex_config: VertexConfig, agg):
        assert self.edge is not None
        edges = agg[self.edge.source, self.edge.target, self.edge.relation]

        # loop over weights for an edge
        for weight_conf in self.edge.weights.vertices:
            vertices = [doc for doc in agg[weight_conf.name]]

            # find all vertices satisfying condition
            if weight_conf.filter:
                vertices = [
                    doc
                    for doc in vertices
                    if all(
                        [
                            doc[q] == v in doc
                            for q, v in weight_conf.filter.items()
                        ]
                    )
                ]
            try:
                doc = next(iter(vertices))
                weight: dict = {}
                if weight_conf.fields:
                    weight = {
                        **weight,
                        **{
                            weight_conf.cfield(field): doc[field]
                            for field in weight_conf.fields
                            if field in doc
                        },
                    }
                if weight_conf.map:
                    weight = {
                        **weight,
                        **{q: doc[k] for k, q in weight_conf.map.items()},
                    }

                if not weight_conf.fields and not weight_conf.map:
                    try:
                        weight = {
                            f"{weight_conf.name}.{k}": doc[k]
                            for k in vertex_config.index(weight_conf.name)
                            if k in doc
                        }
                    except ValueError:
                        weight = {}
                        logger.error(
                            " weights mapper error : weight definition on"
                            f" {self.edge.source} {self.edge.target} refers to"
                            f" a non existent vcollection {weight_conf.name}"
                        )
            except:
                weight = {}
            for edoc in edges:
                edoc.update(weight)
        agg[self.edge.source, self.edge.target, self.edge.relation] = edges
        return agg
