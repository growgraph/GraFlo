"""
parsing tree

"""

from __future__ import annotations

import logging
from collections import defaultdict
from enum import Enum
from itertools import product

from graph_cast.architecture.onto import (
    ANCHOR_KEY,
    SOURCE_AUX,
    TARGET_AUX,
    EdgeMapping,
    TypeVE,
)
from graph_cast.architecture.schema import Edge, VertexConfig
from graph_cast.architecture.transform import Transform
from graph_cast.architecture.util import (
    project_dict,
    project_dicts,
    strip_prefix,
)

logger = logging.getLogger(__name__)


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
        kwargs = strip_prefix(kwargs, "~")
        self.type: NodeType = NodeType(kwargs.pop("type", NodeType.TRIVIAL))
        vertex_config = kwargs.pop("vertex_config", None)

        self.key = kwargs.pop("key", None)

        # validate name wrt vertex_config
        self.collection = kwargs.pop("name", None)

        self._filter: dict | None = kwargs.pop("filter", None)

        self._unfilter: dict | None = kwargs.pop("unfilter", None)

        self.__transforms: list = kwargs.pop("transforms", [])
        self._transforms: list[Transform]

        # TODO : map now is obsolete (taken care by Transform)
        self._map: dict = kwargs.pop("map", {})
        self._anchor: dict = kwargs.pop(ANCHOR_KEY, None)

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
            acc = self._loop_over_children(doc, vertex_config, acc)
        elif self.type == NodeType.VALUE:
            acc[self.collection] += [{self.key: doc}]
        elif self.type == NodeType.MAP:
            if not isinstance(doc, dict):
                raise TypeError(f"at {self} doc is not dict")
            acc = self._apply_map(doc, vertex_config, acc)
        elif self.type == NodeType.LIST:
            if not isinstance(doc, list):
                raise TypeError(f"at {self} doc is not list")
            for item in doc:
                acc = self._loop_over_children(item, vertex_config, acc)
        elif self.type == NodeType.DESCEND:
            if not isinstance(doc, dict):
                raise TypeError(f"at {self} doc is not dict")
            if self.key in doc:
                acc = self._loop_over_children(
                    doc[self.key], vertex_config, acc
                )
        elif self.type == NodeType.EDGE:
            acc = self._add_edges_weights(vertex_config, acc)
        elif self.type == NodeType.WEIGHT:
            acc = self._add_weights(vertex_config, acc)
        else:
            pass

        return acc

    def _loop_over_children(
        self, item, vertex_config, acc
    ) -> defaultdict[TypeVE, list]:
        acc0: defaultdict[TypeVE, list] = defaultdict(list)

        for c in self.children:
            acc0 = c.apply(item, vertex_config, acc0)
        acc = update_defaultdict(acc, acc0)
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
                doc_.update(t(doc, __return_doc=True))

            kkeys += [k for k in self._map if k not in kkeys]

            if isinstance(doc, dict):
                doc_.update({kk: doc[kk] for kk in kkeys if kk in doc})
            if self._map:
                doc_ = {
                    self._map[k] if k in self._map else k: v
                    for k, v in doc_.items()
                }
            if self._anchor is not None:
                doc_.update({ANCHOR_KEY: self._anchor})
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
            source_items, source_index, self.edge._source._anchor
        )
        target_items = pick_indexed_items_anchor_logic(
            target_items, target_index, self.edge._target._anchor
        )

        if self.edge.how == EdgeMapping.ALL:
            for u, v in product(source_items, target_items):
                weight = dict()
                # add `fields` to weight
                for field in self.edge._source.fields:
                    if field.name in u:
                        weight[field.name] = u[field]
                        if field.exclusive:
                            del u[field.name]
                for field in self.edge._target.fields:
                    if field.name in v:
                        weight[field.name] = v[field.name]
                        if field.exclusive:
                            del v[field.name]
                acc[(source, target)] += [
                    {
                        **{
                            SOURCE_AUX: project_dict(u, source_index),
                            TARGET_AUX: project_dict(v, target_index),
                        },
                        **weight,
                    }
                ]
        elif self.edge.how == EdgeMapping.ONE_N:
            source_field, target_field = (
                self.edge._source.selector,
                self.edge._target.selector,
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
                        {
                            field.name: u[field.name]
                            for field in self.edge._source.fields
                            if field.name in u
                        }
                    )
                    up = project_dict(u, source_index)
                    if source_field in u:
                        pointer = u[source_field]
                        if pointer in target_items.keys():
                            acc[(source, target)] += [
                                {
                                    **{
                                        SOURCE_AUX: up,
                                        TARGET_AUX: target_items[pointer],
                                    },
                                    **weight,
                                }
                            ]
                        else:
                            acc[(source, target)] += [
                                {
                                    **{SOURCE_AUX: up, TARGET_AUX: v},
                                    **weight,
                                }
                                for v in target_items.values()
                            ]
                    else:
                        acc[(source, target)] += [
                            {**{SOURCE_AUX: up, TARGET_AUX: v}, **weight}
                            for v in target_items.values()
                        ]
        return acc

    def _add_weights(self, vertex_config: VertexConfig, agg):
        edef = self.edge
        edges = agg[(edef.source, edef.target)]

        # loop over weights for an edge
        for weight_conf in edef.weight_vertices:
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
                            weight_conf.cfield(field.name): doc[field.name]
                            for field in weight_conf.fields
                            if field.name in doc
                        },
                    }
                if weight_conf.mapper:
                    weight = {
                        **weight,
                        **{q: doc[k] for k, q in weight_conf.mapper.items()},
                    }

                if not weight_conf.fields and not weight_conf.mapper:
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
                            f" {edef.source} {edef.target} refers to a non"
                            f" existent vcollection {weight_conf.name}"
                        )
            except:
                weight = {}
            for edoc in edges:
                edoc.update(weight)
        agg[(edef.source, edef.target)] = edges
        return agg


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
    :param anchor: anchor value
    :return: items
    """

    # pick items that have any of index field present
    items_ = [item for item in items if any([k in item for k in indices])]

    # pick items with an anchor key
    if anchor:
        items_ = [
            item
            for item in items_
            if ANCHOR_KEY in item and item[ANCHOR_KEY] == anchor
        ]
    return items_


def update_defaultdict(dd_a: defaultdict, dd_b: defaultdict):
    for k, v in dd_b.items():
        dd_a[k] += v
    return dd_a
