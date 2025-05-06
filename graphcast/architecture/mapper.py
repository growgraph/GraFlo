import dataclasses
import logging
from collections import defaultdict
from itertools import product
from types import MappingProxyType
from typing import Any, Callable, Iterable, Optional

from graphcast.architecture.edge import Edge, EdgeConfig
from graphcast.architecture.extra import update_defaultdict
from graphcast.architecture.onto import (
    DISCRIMINANT_KEY,
    SOURCE_AUX,
    TARGET_AUX,
    EdgeCastingType,
    GraphEntity,
)
from graphcast.architecture.transform import Transform
from graphcast.architecture.util import project_dict
from graphcast.architecture.vertex import (
    VertexConfig,
    VertexRepresentationHelper,
)
from graphcast.onto import BaseDataclass, BaseEnum
from graphcast.util.merge import discriminate

logger = logging.getLogger(__name__)


class NodeType(BaseEnum):
    # only refers to other nodes or maps children nodes to a list
    TRIVIAL = "trivial"
    # adds a vertex specified by a value; terminal
    VALUE = "value"
    # adds a vertex, directly or via a mapping; terminal
    VERTEX = "vertex"
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


# greater numbers have lower priority
NodeTypePriority = MappingProxyType(
    {
        NodeType.VALUE: 30,
        NodeType.VERTEX: 40,
        NodeType.TRIVIAL: 50,
        NodeType.DESCEND: 60,
        NodeType.EDGE: 90,
        NodeType.WEIGHT: 100,
    }
)


@dataclasses.dataclass
class MapperNode(BaseDataclass):
    type: NodeType = NodeType.TRIVIAL
    edge: Optional[Edge] = None
    name: Optional[str] = None
    transforms: list[Transform] = dataclasses.field(default_factory=list)
    keep_fields: list = dataclasses.field(default_factory=list)
    key: Optional[str] = None
    filter: dict = dataclasses.field(default_factory=dict)
    unfilter: dict = dataclasses.field(default_factory=dict)
    map: dict = dataclasses.field(default_factory=dict)
    discriminant: Optional[str] = None
    children: list[dict] = dataclasses.field(default_factory=list)

    def __post_init__(self):
        if self.key is not None:
            assert self.type in {NodeType.DESCEND, NodeType.TRIVIAL, NodeType.VALUE}, (
                f"when key present, NodeType can only be "
                f"{NodeType.DESCEND}, {NodeType.TRIVIAL}, {NodeType.VALUE}"
                f" not provided, for key {self.key} NodeType {self.type} found"
            )
            if self.type == NodeType.TRIVIAL:
                self.type = NodeType.DESCEND
        self._children: list[MapperNode] = [
            MapperNode.from_dict(oo) for oo in self.children
        ]

        self._children = sorted(self._children, key=lambda x: NodeTypePriority[x.type])

    def finish_init(
        self,
        vc: VertexConfig,
        edge_config: EdgeConfig,
        vertex_rep: dict[str, VertexRepresentationHelper],
        transforms: dict[str, Transform],
        same_level_vertices: list[str] | None = None,
        parent_level_key: str | None = None,
    ):
        same_level_vertices = [] if same_level_vertices is None else same_level_vertices
        _same_level_vertices = [
            c.name
            for c in self._children
            if c.type == NodeType.VERTEX and c.name is not None
        ]

        for c in self._children:
            c.finish_init(
                vc,
                edge_config=edge_config,
                vertex_rep=vertex_rep,
                transforms=transforms,
                same_level_vertices=_same_level_vertices,
                parent_level_key=self.key,
            )

        if self.type == NodeType.EDGE:
            if self.edge is not None:
                self.edge.finish_init(vc, same_level_vertices)
                edge_config.update_edges(self.edge)
        elif self.type == NodeType.VERTEX or self.type == NodeType.VALUE:
            dummy_transforms = [t for t in self.transforms if t.is_dummy]
            valid_transforms = [t for t in self.transforms if not t.is_dummy]

            while dummy_transforms:
                t = dummy_transforms.pop()
                if t.name is not None and t.name in transforms:
                    valid_transforms += [t.update(transforms[t.name])]

            self.transforms = valid_transforms

            if self.discriminant is not None:
                assert self.name is not None
                vc.discriminant_chart[self.name] = True

            if self.type == NodeType.VERTEX:
                if self.name not in vertex_rep:
                    assert self.name is not None
                    vertex_rep[self.name] = VertexRepresentationHelper(
                        name=self.name, fields=vc.fields(self.name)
                    )
                    if self.map:
                        vertex_rep[self.name].maps += [dict(self.map)]
                    for t in self.transforms:
                        vertex_rep[self.name].transforms += [(t.input, t.output)]
            elif self.type == NodeType.VALUE:
                if self.key is None:
                    self.key = parent_level_key

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
                return flag and any([doc[k] != v for k, v in self.unfilter.items()])
        else:
            return any([doc[k] != v for k, v in self.unfilter.items()])

    def apply(
        self, doc, vertex_config: VertexConfig, acc: defaultdict[GraphEntity, list]
    ):
        if self.type == NodeType.TRIVIAL:
            if isinstance(doc, list):
                for sub_doc in doc:
                    acc = self._loop_over_children(sub_doc, vertex_config, acc)
            else:
                acc = self._loop_over_children(doc, vertex_config, acc)
        elif self.type == NodeType.DESCEND:
            if self.key in doc:
                sub_doc = doc[self.key]
                if isinstance(sub_doc, list):
                    for ssub_doc in sub_doc:
                        acc = self._loop_over_children(ssub_doc, vertex_config, acc)
                else:
                    acc = self._loop_over_children(sub_doc, vertex_config, acc)
        elif self.type == NodeType.VALUE:
            acc = self._apply_value(doc, acc)
        elif self.type == NodeType.VERTEX:
            if not isinstance(doc, dict):
                raise TypeError(f"at {self} doc is not dict")
            acc = self._apply_map(doc, vertex_config, acc)
        elif self.type == NodeType.EDGE:
            acc = self._add_edges(
                vertex_config,
                acc,
            )
        elif self.type == NodeType.WEIGHT:
            acc = self._add_weights(vertex_config, acc)
        else:
            pass
        return acc

    def __repr__(self):
        s = f"(type = {self.type}, "
        s += f"name = `{self.name}`, "
        s += f"descend = `{self.key}`, "
        s += f"edge = {self.edge}"
        s += ")"
        return s

    def _loop_over_children(
        self, item, vertex_config, acc
    ) -> defaultdict[GraphEntity, list]:
        acc0: defaultdict[GraphEntity, list] = defaultdict(list)

        for c in self._children:
            acc0 = c.apply(item, vertex_config, acc0)
        acc = update_defaultdict(acc, acc0)
        return acc

    def _apply_map(
        self,
        doc,
        vertex_config: VertexConfig,
        acc: defaultdict[GraphEntity, list],
    ):
        if self.passes(doc) and self.name is not None:
            keys = vertex_config.fields(self.name)
            _doc = dict()
            for t in self.transforms:
                _doc.update(t(doc, __return_doc=True))

            keys += [k for k in self.map if k not in keys]

            if isinstance(doc, dict):
                _doc.update(
                    {kk: doc[kk] for kk in keys if kk in doc and kk not in _doc}
                )
            if self.map:
                _doc = {self.map[k] if k in self.map else k: v for k, v in _doc.items()}
            if self.discriminant is not None:
                _doc.update({DISCRIMINANT_KEY: self.discriminant})
            if self.keep_fields is not None:
                _doc.update({f: doc[f] for f in self.keep_fields})
            acc[self.name] += [_doc]
        return acc

    def _apply_value(self, doc, acc: defaultdict[GraphEntity, list]):
        value = doc
        for t in self.transforms:
            value = t(doc, __return_doc=False)
        if self.name is not None:
            acc[self.name] += [{self.key: value}]
        return acc

    def _add_edges(
        self,
        vertex_config: VertexConfig,
        acc: defaultdict[GraphEntity, list],
    ):
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
            DISCRIMINANT_KEY if vertex_config.discriminant_chart[source] else None,
            self.edge.source_discriminant,
        )

        target_items = discriminate(
            target_items,
            target_index,
            DISCRIMINANT_KEY if vertex_config.discriminant_chart[target] else None,
            self.edge.target_discriminant,
        )

        if source == target:
            # in the rare case when the relation is between the vertex types and the discriminant value is not set
            # for one the groups, we make them disjoint

            if (
                self.edge.source_discriminant is not None
                and self.edge.target_discriminant is None
            ):
                target_items = [
                    item
                    for item in target_items
                    if DISCRIMINANT_KEY not in item
                    or item[DISCRIMINANT_KEY] != self.edge.source_discriminant
                ]

            elif (
                self.edge.source_discriminant is None
                and self.edge.target_discriminant is not None
            ):
                source_items = [
                    item
                    for item in source_items
                    if DISCRIMINANT_KEY is not item
                    or item[DISCRIMINANT_KEY] != self.edge.target_discriminant
                ]

        if self.edge.casting_type == EdgeCastingType.PAIR_LIKE:
            iterator: Callable[..., Iterable[Any]] = zip
        else:
            iterator = product

        relation = self.edge.relation

        for u, v in iterator(source_items, target_items):
            # adding weight from source or target
            weight = dict()
            if self.edge.weights:
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

            if self.edge.source_relation_field is not None:
                relation = u.pop(self.edge.source_relation_field, None)
            if self.edge.target_relation_field is not None:
                relation = v.pop(self.edge.target_relation_field, None)

            acc[source, target, relation] += [
                {
                    **{
                        SOURCE_AUX: project_dict(u, source_index),
                        TARGET_AUX: project_dict(v, target_index),
                    },
                    **weight,
                }
            ]
        return acc

    def _add_weights(self, vertex_config: VertexConfig, agg):
        assert self.edge is not None
        edges = agg[self.edge.source, self.edge.target, self.edge.relation]

        # loop over weights for an edge
        vertices = self.edge.weights.vertices if self.edge.weights is not None else []
        for weight_conf in vertices:
            vertices = [doc for doc in agg[weight_conf.name]]

            # find all vertices satisfying condition
            if weight_conf.filter:
                vertices = [
                    doc
                    for doc in vertices
                    if all([doc[q] == v in doc for q, v in weight_conf.filter.items()])
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
