from __future__ import annotations

import dataclasses
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from itertools import product
from types import MappingProxyType
from typing import Any, Callable, Iterable, Optional

from dataclass_wizard import JSONWizard

from graphcast.architecture.edge import Edge, EdgeConfig
from graphcast.architecture.onto import (
    DISCRIMINANT_KEY,
    SOURCE_AUX,
    TARGET_AUX,
    EdgeCastingType,
    EncodingType,
    GraphEntity,
)
from graphcast.architecture.resource_util import (
    add_blank_collections,
    apply_filter,
    define_edges,
)
from graphcast.architecture.transform import Transform
from graphcast.architecture.util import project_dict
from graphcast.architecture.vertex import (
    VertexConfig,
    VertexRepresentationHelper,
)
from graphcast.onto import BaseDataclass
from graphcast.util.merge import discriminate, merge_doc_basis
from graphcast.util.transform import pick_unique_dict

logger = logging.getLogger(__name__)


DESCEND_KEY_VALUES = {"key"}
DUMMY_KEY = "__dummy_key__"
DRESSING_TRANSFORMED_VALUE_KEY = "__value__"


@dataclasses.dataclass(kw_only=True)
class AbsActionContext(BaseDataclass, ABC):
    acc: defaultdict[GraphEntity, list] = dataclasses.field(
        default_factory=lambda: defaultdict(list)
    )
    vertex_buffer: defaultdict[GraphEntity, dict] = dataclasses.field(
        default_factory=lambda: defaultdict(dict)
    )
    cdoc: dict = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class ActionContext(AbsActionContext):
    doc: dict | list


@dataclasses.dataclass
class ActionContextPure(AbsActionContext):
    doc: dict


class ActionNode(ABC):
    @abstractmethod
    def __call__(self, ctx: ActionContext, **kwargs):
        pass

    def finish_init(self, **kwargs):
        pass


class VertexNode(ActionNode):
    def __init__(
        self,
        vertex: str,
        discriminant: Optional[str] = None,
        keep_fields: Optional[tuple[str]] = None,
        **kwargs,
    ):
        self.name = vertex
        self.discriminant: Optional[str] = discriminant
        self.keep_fields: Optional[tuple[str]] = keep_fields
        self.vertex_config: VertexConfig

    def finish_init(self, **kwargs):
        self.vertex_config: VertexConfig = kwargs.pop("vertex_config")
        self.vertex_config.discriminant_chart[self.name] = True

    def __call__(self, ctx: ActionContextPure, **kwargs):
        # take relevant fields from doc if available, otherwise try DRESSING_TRANSFORMED_VALUE_KEY
        vertex_keys = self.vertex_config.fields(self.name)

        cdoc: dict
        if self.name in ctx.vertex_buffer:
            cdoc = ctx.vertex_buffer[self.name]
            cdoc.update({k: v for k, v in ctx.cdoc.items() if k not in cdoc})
            del ctx.vertex_buffer[self.name]
        else:
            cdoc = ctx.cdoc

        if isinstance(ctx.doc, dict):
            cdoc.update(
                {k: v for k, v in ctx.doc.items() if k not in cdoc and k in vertex_keys}
            )
        else:
            # if ctx.doc is not a dict, it is an indication that it is the lowest level, and it was processed as value
            remap = {
                f"{DRESSING_TRANSFORMED_VALUE_KEY}#{j}": f
                for j, f in enumerate(self.vertex_config.index(self.name).fields)
            }
            cdoc = {remap[k]: v for k, v in cdoc.items()}

        _doc = {k: cdoc[k] for k in vertex_keys if k in cdoc}
        if self.discriminant is not None:
            _doc.update({DISCRIMINANT_KEY: self.discriminant})
        if self.keep_fields is not None:
            _doc.update({f: ctx.doc[f] for f in self.keep_fields if f in ctx.doc})
        ctx.acc[self.name] += [_doc]
        return ctx


class EdgeNode(ActionNode):
    def __init__(
        self,
        **kwargs,
    ):
        self.edge = Edge.from_dict(kwargs)
        self.vertex_config: VertexConfig

    def finish_init(self, **kwargs):
        self.vertex_config: VertexConfig = kwargs.pop("vertex_config")

    def __call__(self, ctx: ActionContextPure, **kwargs):
        # get source and target names
        source, target = self.edge.source, self.edge.target

        # get source and target edge fields
        source_index, target_index = (
            self.vertex_config.index(source),
            self.vertex_config.index(target),
        )

        # get source and target items
        source_items, target_items = ctx.acc[source], ctx.acc[target]

        source_items = discriminate(
            source_items,
            source_index,
            DISCRIMINANT_KEY if self.vertex_config.discriminant_chart[source] else None,
            self.edge.source_discriminant,
        )

        target_items = discriminate(
            target_items,
            target_index,
            DISCRIMINANT_KEY if self.vertex_config.discriminant_chart[target] else None,
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

        edges = []

        for u, v in iterator(source_items, target_items):
            # adding weight from source or target
            weight = dict()
            if self.edge.weights is not None:
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

            edges += [
                {
                    **{
                        SOURCE_AUX: project_dict(u, source_index),
                        TARGET_AUX: project_dict(v, target_index),
                    },
                    **weight,
                }
            ]
        edges = self._add_weights(edges, ctx)
        ctx.acc[source, target, relation] = self._add_weights(edges, ctx)
        return ctx

    def _add_weights(self, edges, ctx: ActionContextPure):
        acc = ctx.acc
        vertices = [] if self.edge.weights is None else self.edge.weights.vertices
        for weight_conf in vertices:
            vertices = [doc for doc in acc[weight_conf.name]]

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
                            for k in self.vertex_config.index(weight_conf.name)
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
        return edges


class TransformNode(ActionNode):
    def __init__(self, **kwargs):
        self.vertex: Optional[str] = kwargs.pop("target_vertex", None)
        self.transforms: dict = kwargs.pop("transforms", {})
        self.name = kwargs.get("name", None)
        self.t = Transform(**kwargs)
        logging.debug(f"transforms : {id(self.transforms)} {len(self.transforms)}")

    def finish_init(self, **kwargs):
        self.transforms = kwargs.pop("transforms", {})
        if self.name is not None and not self.t.is_dummy:
            self.transforms[self.name] = self.t

    def __call__(self, ctx: ActionContextPure, **kwargs):
        logging.debug(f"transforms : {id(self.transforms)} {len(self.transforms)}")
        if self.name is not None:
            t = self.transforms[self.name]
        else:
            t = self.t

        _update_doc: dict
        if isinstance(ctx.doc, dict):
            _update_doc = t(ctx.doc, __return_doc=True, **kwargs)
        else:
            value = t(ctx.doc, __return_doc=False, **kwargs)
            if isinstance(value, tuple):
                _update_doc = {
                    f"{DRESSING_TRANSFORMED_VALUE_KEY}#{j}": v
                    for j, v in enumerate(value)
                }
            else:
                _update_doc = {f"{DRESSING_TRANSFORMED_VALUE_KEY}#0": value}
        if self.vertex is None:
            ctx.cdoc.update(_update_doc)
        else:
            ctx.vertex_buffer[self.vertex] = _update_doc
        return ctx


class DescendNode(ActionNode):
    def __init__(self, key: Optional[str], descendants_kwargs: list, **kwargs):
        self.key = key
        self.descendants: list[ActionNodeWrapper] = []
        for descendant_kwargs in descendants_kwargs:
            self.descendants += [ActionNodeWrapper(**descendant_kwargs, **kwargs)]

        self.descendants = sorted(
            self.descendants, key=lambda x: _NodeTypePriority[type(x.action_node)]
        )

        logger.debug(
            f"""type, priority: {
                [
                    (t.__name__, _NodeTypePriority[t])
                    for t in (type(x.action_node) for x in self.descendants)
                ]
            }"""
        )

    def finish_init(self, **kwargs):
        for an in self.descendants:
            an.finish_init(**kwargs)

    def __call__(self, ctx: ActionContext, **kwargs):
        if isinstance(ctx.doc, dict) and self.key in ctx.doc:
            ctx.doc = ctx.doc[self.key]
        elif self.key is not None:
            logging.error(f"doc {ctx.doc} was expected to have level {self.key}")

        doc_level = ctx.doc if isinstance(ctx.doc, list) else [ctx.doc]

        for sub_doc in doc_level:
            ctx.doc = sub_doc
            ctx.cdoc = {}
            for anw in self.descendants:
                ctx = anw(ctx)
        return ctx


_NodeTypePriority = MappingProxyType(
    {
        TransformNode: 20,
        VertexNode: 50,
        DescendNode: 60,
        EdgeNode: 90,
    }
)


class ActionNodeWrapper:
    def __init__(self, *args, **kwargs):
        self.action_node: ActionNode
        if self._try_init_descend_node(*args, **kwargs):
            pass
        elif self._try_init_transform_node(**kwargs):
            pass
        elif self._try_init_vertex_node(**kwargs):
            pass
        elif self._try_init_edge_node(**kwargs):
            pass
        else:
            raise ValueError(f"Not able to init ActionNodeWrapper with {kwargs}")

    def finish_init(self, **kwargs):
        kwargs["transforms"] = kwargs.get("transforms", {})
        self.vertex_config = kwargs.get("vertex_config", VertexConfig(vertices=[]))
        self.action_node.finish_init(**kwargs)

    def _try_init_descend_node(self, *args, **kwargs) -> bool:
        descend_key_candidates = [kwargs.pop(k, None) for k in DESCEND_KEY_VALUES]
        descend_key_candidates = [x for x in descend_key_candidates if x is not None]
        descend_key = descend_key_candidates[0] if descend_key_candidates else None
        ds = kwargs.pop("apply", None)
        if ds is not None:
            if isinstance(ds, list):
                descendants = ds
            else:
                descendants = [ds]
        elif len(args) > 0:
            descendants = list(args)
        else:
            return False
        self.action_node = DescendNode(
            descend_key, descendants_kwargs=descendants, **kwargs
        )
        return True

    def _try_init_transform_node(self, **kwargs) -> bool:
        try:
            self.action_node = TransformNode(**kwargs)
            return True
        except Exception:
            return False

    def _try_init_vertex_node(self, **kwargs) -> bool:
        try:
            self.action_node = VertexNode(**kwargs)
            return True
        except Exception:
            return False

    def _try_init_edge_node(self, **kwargs) -> bool:
        try:
            self.action_node = EdgeNode(**kwargs)
            return True
        except Exception:
            return False

    def __call__(
        self,
        ctx: ActionContext,
    ) -> ActionContext:
        ctx = self.action_node(ctx)
        return ctx

    def normalize_unit(
        self, ctx: ActionContext, edges: list[Edge]
    ) -> defaultdict[GraphEntity, list]:
        unit_doc = ctx.acc

        for vertex, v in unit_doc.items():
            v = pick_unique_dict(v)
            if vertex in self.vertex_config.vertex_set:
                v = merge_doc_basis(
                    v,
                    tuple(self.vertex_config.index(vertex).fields),
                    DISCRIMINANT_KEY
                    if self.vertex_config.discriminant_chart[vertex]
                    else None,
                )
                for item in v:
                    item.pop(DISCRIMINANT_KEY, None)
            unit_doc[vertex] = v

        unit_doc = add_blank_collections(unit_doc, self.vertex_config)

        unit_doc = apply_filter(unit_doc, vertex_conf=self.vertex_config)

        # pure_weight = extract_weights(unit_doc, edge_config.edges)

        unit_doc = define_edges(
            unit=unit_doc,
            unit_weights=defaultdict(),
            current_edges=edges,
            vertex_conf=self.vertex_config,
        )

        return unit_doc

    @classmethod
    def from_dict(cls, data: dict | list):
        if isinstance(data, list):
            return cls(*data)
        else:
            return cls(**data)


@dataclasses.dataclass(kw_only=True)
class SimpleResource(BaseDataclass, JSONWizard):
    resource_name: str
    apply: list
    encoding: EncodingType = EncodingType.UTF_8
    merge_collections: list[str] = dataclasses.field(default_factory=list)
    extra_weights: list[Edge] = dataclasses.field(default_factory=list)
    types: dict[str, str] = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        self.root = ActionNodeWrapper(*self.apply)
        self.vertex_rep: dict[str, VertexRepresentationHelper] = dict()
        self.name = self.resource_name
        self._types: dict[str, Callable] = dict()
        for k, v in self.types.items():
            try:
                self._types[k] = eval(v)
            except Exception as ex:
                logger.error(
                    f"For resource {self.name} for field {k} failed to cast type {v} : {ex}"
                )

    def finish_init(
        self,
        vc: VertexConfig,
        edge_config: EdgeConfig,
        transforms: dict[str, Transform],
    ):
        self.root.finish_init(
            vc,
            edge_config=edge_config,
            vertex_rep=self.vertex_rep,
            transforms=transforms,
        )
        for e in self.extra_weights:
            e.finish_init(vc)

    def __call__(self, doc: dict) -> defaultdict[GraphEntity, list]:
        ctx = ActionContext(doc=doc)
        ctx = self.root(
            ctx,
        )
        # acc = self.normalize_unit(acc, vertex_config)

        return ctx.acc
