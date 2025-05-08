import dataclasses
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from itertools import product
from types import MappingProxyType
from typing import Any, Callable, Iterable, Optional

from graphcast.architecture.edge import Edge, EdgeConfig
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
)
from graphcast.architecture.wrapper import ActorWrapper
from graphcast.onto import BaseDataclass
from graphcast.util.merge import discriminate

logger = logging.getLogger(__name__)


DESCEND_KEY_VALUES = {"key"}
DRESSING_TRANSFORMED_VALUE_KEY = "__value__"


@dataclasses.dataclass(kw_only=True)
class ActionContextAbstract(BaseDataclass, ABC):
    # accumulation of vertices and edges
    acc: defaultdict[GraphEntity, list] = dataclasses.field(
        default_factory=lambda: defaultdict(list)
    )
    vertex_buffer: defaultdict[GraphEntity, dict] = dataclasses.field(
        default_factory=lambda: defaultdict(dict)
    )
    # current doc : the result of application of transformations to the original document
    cdoc: dict = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class ActionContext(ActionContextAbstract):
    # current level of the document under analysis, either a dict or a list
    doc: dict | list


@dataclasses.dataclass
class ActionContextPure(ActionContextAbstract):
    # current level of the document under analysis, a dict
    doc: dict


class Actor(ABC):
    @abstractmethod
    def __call__(self, ctx: ActionContext, **kwargs):
        pass

    def finish_init(self, **kwargs):
        pass


class VertexActor(Actor):
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


class EdgeActor(Actor):
    def __init__(
        self,
        **kwargs,
    ):
        self.edge = Edge.from_dict(kwargs)
        self.vertex_config: VertexConfig

    def finish_init(self, **kwargs):
        self.vertex_config: VertexConfig = kwargs.pop("vertex_config")
        edge_config: EdgeConfig = kwargs.pop("edge_config")
        if self in edge_config:
            # TODO add edges
            pass

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


class TransformActor(Actor):
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


class DescendActor(Actor):
    def __init__(self, key: Optional[str], descendants_kwargs: list, **kwargs):
        self.key = key
        self.descendants: list[ActorWrapper] = []
        for descendant_kwargs in descendants_kwargs:
            self.descendants += [ActorWrapper(**descendant_kwargs, **kwargs)]

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
        TransformActor: 20,
        VertexActor: 50,
        DescendActor: 60,
        EdgeActor: 90,
    }
)
