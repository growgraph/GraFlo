"""
parsing tree

"""

from __future__ import annotations

import logging
from collections import defaultdict
from enum import Enum
from itertools import product

from graph_cast.architecture.general import transform_foo
from graph_cast.architecture.schema import (
    Edge,
    EdgeMapping,
    TypeVE,
    VertexConfig,
    _anchor_key,
    _source_aux,
    _target_aux,
)
from graph_cast.architecture.transform import Transform
from graph_cast.architecture.uitl import project_dict, project_dicts

logger = logging.getLogger(__name__)

xml_dummy = "#text"


class NodeType(str, Enum):
    # only refers to other nodes
    TRIVIAL = "trivial"
    # adds a vertex specified by a value; terminal
    VALUE = "value"
    # adds a vertex, directly or via a mapping; terminal
    MAP = "dict"
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


class MapperNode:
    def __init__(self, **kwargs):
        self.type: NodeType = NodeType(kwargs.pop("type", NodeType.TRIVIAL))
        vertex_config = kwargs.pop("vertex_config", None)

        self.key = kwargs.pop("key", None)

        self.collection = kwargs.pop("name", None)

        self._filter: dict | None = kwargs.pop("filter", None)

        self._unfilter: dict | None = kwargs.pop("unfilter", None)

        self.__transforms: list = kwargs.pop("transforms", [])
        self._transforms: list[Transform]

        self._map: dict = kwargs.pop("map", {})
        self._anchor: dict = kwargs.pop(_anchor_key, None)

        self._init_transforms()

        # only for edges
        self.edge: Edge
        if self.type in [NodeType.EDGE, NodeType.WEIGHT]:
            self.edge = Edge(kwargs, vconf=vertex_config)

        children_defs: list = kwargs.pop("maps", [])
        if not isinstance(children_defs, (list, tuple)):
            raise TypeError(
                "config file does not comply : should be list under `maps`"
            )
        self.children: list[MapperNode] = [
            MapperNode(vertex_config=vertex_config, **qwargs)
            for qwargs in children_defs
        ]
        self.children = sorted(self.children, key=lambda x: x.type)

    def _init_transforms(self):
        self._transforms = []
        for t in self.__transforms:
            try:
                t_ = Transform(**t)
                self._transforms.append(t_)
            except:
                RuntimeError(f"Transform {t} failed to init")

    @property
    def transforms(self):
        return (x for x in self._transforms)

    def is_leaf(self):
        return True if self.children else False

    @property
    def no_filters(self):
        return self._filter is None and self._unfilter is None

    def filter(self, doc):
        if self._filter is not None:
            return all([doc[k] == v for k, v in self._filter.items()])
        return False

    def unfilter(self, doc):
        if self._unfilter is not None:
            return any([doc[k] != v for k, v in self._unfilter.items()])
        return False

    def apply(
        self,
        doc,
        vertex_config: VertexConfig,
        acc: defaultdict[TypeVE, list],
    ) -> defaultdict[TypeVE, list]:
        if self.type == NodeType.TRIVIAL:
            for c in self.children:
                acc = c.apply(doc, vertex_config, acc)
        elif self.type == NodeType.VALUE:
            acc[self.collection] += [{self.key: doc}]
        elif self.type == NodeType.MAP:
            acc = self._apply_map(doc, vertex_config, acc)
        elif self.type == NodeType.LIST:
            for item in doc:
                for c in self.children:
                    acc = c.apply(item, vertex_config, acc)
        elif self.type == NodeType.DESCEND:
            if self.key in doc:
                for c in self.children:
                    acc = c.apply(doc[self.key], vertex_config, acc)
        elif self.type == NodeType.EDGE:
            acc = self._add_edges_weights(vertex_config, acc)
        elif self.type == NodeType.WEIGHT:
            pass
        else:
            pass

        return acc

    @property
    def n_children(self):
        return len(self.children)

    def __repr__(self):
        s = f"(type = {self.type}, "
        s += f"descend = {self.key}"
        s += f")"
        return s

    def _apply_map(
        self,
        doc,
        vertex_config: VertexConfig,
        acc: defaultdict[TypeVE, list],
    ) -> defaultdict[TypeVE, list]:
        if self.no_filters or self.filter(doc) or self.unfilter(doc):
            kkeys = vertex_config.fields(self.collection)
            doc_ = dict()
            for t in self.transforms:
                doc_.update(transform_foo(t, doc))

            kkeys += [k for k in self._map if k not in kkeys]

            if isinstance(doc, dict):
                for kk, vv in doc.items():
                    if kk in kkeys:
                        if isinstance(vv, dict):
                            if xml_dummy in vv:
                                doc_[kk] = vv[xml_dummy]
                        else:
                            doc_[kk] = vv
            if self._map:
                doc_ = {
                    self._map[k] if k in self._map else k: v
                    for k, v in doc_.items()
                    if v
                }
            if self._anchor is not None:
                doc_.update({_anchor_key: self._anchor})
            acc[self.collection] += [doc_]
        return acc

    def _add_edges_weights(self, vertex_config: VertexConfig, acc):
        # get source and target names
        source, target = self.edge.source, self.edge.target

        # get source and target edge fields
        source_index, target_index = (
            vertex_config.index(source),
            vertex_config.index(target),
        )

        # get source and target items
        source_items, target_items = acc[source], acc[target]

        source_items = pick_indexed_items_anchor_logic(
            source_items, source_index, self.edge._source.anchor
        )
        target_items = pick_indexed_items_anchor_logic(
            target_items, target_index, self.edge._target.anchor
        )

        if self.edge.how == EdgeMapping.ALL:
            for u, v in product(source_items, target_items):
                weight = dict()
                # add `fields` to weight
                for k in self.edge._source.fields:
                    if k in u:
                        weight[k] = u[k]
                for k in self.edge._target.fields:
                    if k in v:
                        weight[k] = v[k]
                # move `weight_exclusive` to weight
                for k in self.edge._source.weight_exclusive:
                    if k in u:
                        weight[k] = u[k]
                        del u[k]
                for k in self.edge._target.weight_exclusive:
                    if k in v:
                        weight[k] = v[k]
                        del v[k]
                acc[(source, target)] += [
                    {
                        **{
                            _source_aux: project_dict(u, source_index),
                            _target_aux: project_dict(v, target_index),
                        },
                        **weight,
                    }
                ]
        elif self.edge.how == EdgeMapping.ONE_N:
            source_field, target_field = (
                self.edge._source.field,
                self.edge._target.field,
            )

            target_items = [
                item for item in target_items if target_field in item
            ]

            if target_items:
                target_items = dict(
                    zip(
                        [item[target_field] for item in target_items],
                        project_dicts(target_items, target_index),
                    )
                )
                for u in source_items:
                    weight = dict()
                    weight.update(
                        {k: u[k] for k in self.edge._source.fields if k in u}
                    )
                    up = project_dict(u, source_index)
                    if source_field in u:
                        pointer = u[source_field]
                        if pointer in target_items.keys():
                            acc[(source, target)] += [
                                {
                                    **{
                                        _source_aux: up,
                                        _target_aux: target_items[pointer],
                                    },
                                    **weight,
                                }
                            ]
                        else:
                            acc[(source, target)] += [
                                {
                                    **{_source_aux: up, _target_aux: v},
                                    **weight,
                                }
                                for v in target_items.values()
                            ]
                    else:
                        acc[(source, target)] += [
                            {**{_source_aux: up, _target_aux: v}, **weight}
                            for v in target_items.values()
                        ]
        return acc


class NodeFactory:
    @staticmethod
    def render_node(self):
        pass


class ParsingTree:
    def __init__(self, config=None, vertex_config=None):
        self.root = MapperNode(vertex_config=vertex_config, **config)

    def __repr__(self):
        def edges_string(n: MapperNode, s: str, ind: str):
            ind += "\t"
            for c in n.children:
                s += f"{ind}{n} -> {c}\n"
                s = edges_string(c, s, str(ind))
            return s

        s = ""
        indent = ""
        s = edges_string(self.root, s, indent)
        return s

    def apply(self, doc, vertex_config) -> defaultdict[TypeVE, list]:
        acc: defaultdict[TypeVE, list] = defaultdict(list)
        return self.root.apply(doc, vertex_config=vertex_config, acc=acc)


def pick_indexed_items_anchor_logic(items, indices, anchor):
    """

    :param items: list of documents (dict)
    :param indices:
    :param anchor:
    :return: items
    """

    # pick items that have any of index field present
    items_ = [item for item in items if any([k in item for k in indices])]

    # pick items with an anchor key
    if anchor:
        items_ = [
            item
            for item in items_
            if _anchor_key in item and item[_anchor_key] == anchor
        ]
    return items_
