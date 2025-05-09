from __future__ import annotations

import dataclasses
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from itertools import product
from types import MappingProxyType
from typing import Any, Callable, Iterable, Optional, Type, TypeVar

from graphcast.architecture.edge import Edge, EdgeConfig
from graphcast.architecture.onto import (
    DISCRIMINANT_KEY,
    SOURCE_AUX,
    TARGET_AUX,
    EdgeCastingType,
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
)
from graphcast.onto import BaseDataclass
from graphcast.util.merge import discriminate, merge_doc_basis
from graphcast.util.transform import pick_unique_dict

logger = logging.getLogger(__name__)


DESCEND_KEY_VALUES = {"key"}
DRESSING_TRANSFORMED_VALUE_KEY = "__value__"


@dataclasses.dataclass(kw_only=True)
class ActionContext(BaseDataclass):
    # accumulation of vertices and edges
    acc: defaultdict[GraphEntity, list] = dataclasses.field(
        default_factory=lambda: defaultdict(list)
    )
    vertex_buffer: defaultdict[GraphEntity, dict] = dataclasses.field(
        default_factory=lambda: defaultdict(dict)
    )
    # current doc : the result of application of transformations to the original document
    cdoc: dict = dataclasses.field(default_factory=dict)


class Actor(ABC):
    @abstractmethod
    def __call__(self, ctx: ActionContext, *nargs, **kwargs):
        pass

    def finish_init(self, **kwargs):
        pass


ActorType = TypeVar("ActorType", bound=Actor)


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

    def __call__(self, ctx: ActionContext, *nargs, **kwargs):
        doc: dict = kwargs.pop("doc", None)

        # take relevant fields from doc if available, otherwise try DRESSING_TRANSFORMED_VALUE_KEY
        vertex_keys = self.vertex_config.fields(self.name)

        cdoc: dict
        if self.name in ctx.vertex_buffer:
            cdoc = ctx.vertex_buffer[self.name]
            cdoc.update({k: v for k, v in ctx.cdoc.items() if k not in cdoc})
            del ctx.vertex_buffer[self.name]
        else:
            cdoc = ctx.cdoc

        if doc is not None and isinstance(doc, dict):
            cdoc.update(
                {k: v for k, v in doc.items() if k not in cdoc and k in vertex_keys}
            )
        else:
            # if doc is not a dict, it is an indication that it is the lowest level, and it was processed as value
            remap = {
                f"{DRESSING_TRANSFORMED_VALUE_KEY}#{j}": f
                for j, f in enumerate(self.vertex_config.index(self.name).fields)
            }
            cdoc = {remap[k]: v for k, v in cdoc.items()}

        _doc = {k: cdoc[k] for k in vertex_keys if k in cdoc}
        if self.discriminant is not None:
            _doc.update({DISCRIMINANT_KEY: self.discriminant})
        if self.keep_fields is not None:
            _doc.update({f: doc[f] for f in self.keep_fields if f in doc})
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

        # TODO reintroduce same_level_vertices
        same_level_vertices = []
        if self.edge not in edge_config:
            self.edge.finish_init(self.vertex_config, same_level_vertices)
            edge_config.update_edges(self.edge)

    def __call__(self, ctx: ActionContext, *nargs, **kwargs):
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
            # in the rare case when relation is an auto relation (instances of the same vertex)
            # the discriminant value is not set
            # we make them disjoint

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

    def _add_weights(self, edges, ctx: ActionContext):
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
        self.transforms: dict
        self.name = kwargs.get("name", None)
        self.t = Transform(**kwargs)

    def finish_init(self, **kwargs):
        self.transforms = kwargs.pop("transforms", {})
        if self.name is not None and not self.t.is_dummy:
            self.transforms[self.name] = self.t

    def __call__(self, ctx: ActionContext, *nargs, **kwargs):
        logging.debug(f"transforms : {id(self.transforms)} {len(self.transforms)}")

        if kwargs:
            doc: Optional[dict] = kwargs.get("doc")
        elif nargs:
            doc = nargs[0]
        else:
            raise ValueError(f"{type(self).__name__}: doc should be provided")

        if self.name is not None:
            t = self.transforms[self.name]
        else:
            t = self.t

        _update_doc: dict
        if isinstance(doc, dict):
            _update_doc = t(doc, __return_doc=True)
        else:
            value = t(doc, __return_doc=False)
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
        self._descendants: list[ActorWrapper] = []
        for descendant_kwargs in descendants_kwargs:
            self._descendants += [ActorWrapper(**descendant_kwargs, **kwargs)]

    def add_descendant(self, d: ActorWrapper):
        self._descendants += [d]

    @property
    def descendants(self) -> list[ActorWrapper]:
        return sorted(self._descendants, key=lambda x: _NodeTypePriority[type(x.actor)])

    def finish_init(self, **kwargs):
        self.vertex_config: VertexConfig = kwargs.get(
            "vertex_config", VertexConfig(vertices=[])
        )

        for an in self.descendants:
            an.finish_init(**kwargs)

        # autofill vertices
        # 1. check all transforms
        # 2. find matching with vertex fields
        # 3. add vertices

        available_fields = set()
        for anw in self.descendants:
            actor = anw.actor
            if isinstance(actor, TransformActor):
                available_fields |= set(list(actor.t.output))

        present_vertices = [
            anw.actor.name
            for anw in self.descendants
            if isinstance(anw.actor, VertexActor)
        ]

        for v in self.vertex_config.vertices:
            intersection = available_fields & set(v.fields)
            if intersection and v.name not in present_vertices:
                new_descendant = ActorWrapper(vertex=v.name)
                new_descendant.finish_init(**kwargs)
                self.add_descendant(new_descendant)

        normalizer = ActorWrapper(normalizer=True)
        normalizer.finish_init(**kwargs)

        self.add_descendant(normalizer)

        logger.debug(
            f"""type, priority: {
                [
                    (t.__name__, _NodeTypePriority[t])
                    for t in (type(x.actor) for x in self.descendants)
                ]
            }"""
        )

    def __call__(self, ctx: ActionContext, **kwargs):
        doc = kwargs.pop("doc")

        if doc is None:
            raise ValueError(f"{type(self).__name__}: doc should be provided")

        if isinstance(doc, dict) and self.key in doc:
            doc = doc[self.key]
        elif self.key is not None:
            logging.error(f"doc {doc} was expected to have level {self.key}")

        doc_level = doc if isinstance(doc, list) else [doc]

        logger.debug(f"{len(doc_level)}")

        for i, sub_doc in enumerate(doc_level):
            logger.debug(f"docs: {i + 1}/{len(doc_level)}")
            if isinstance(sub_doc, dict):
                nargs: tuple = tuple()
                kwargs["doc"] = sub_doc
            else:
                nargs = (sub_doc,)
            ctx.cdoc = {}
            for j, anw in enumerate(self.descendants):
                logger.debug(
                    f"{type(anw.actor).__name__}: {j + 1}/{len(self.descendants)}"
                )
                ctx = anw(ctx, *nargs, **kwargs)
        ctx.cdoc = {}
        return ctx


class NormalizerActor(Actor):
    """
    auxiliary actor, needed to merge docs that represent the same vertex
    it should be run before EdgeActor to avoid ambiguous edges
    """

    def __init__(self, normalizer):
        if normalizer is not True:
            raise ValueError("Not a normalizer")

    def finish_init(self, **kwargs):
        self.vertex_config: VertexConfig = kwargs.pop("vertex_config")

    def __call__(self, ctx: ActionContext, *nargs, **kwargs):
        unit = ctx.acc

        for vertex, v in unit.items():
            v = pick_unique_dict(v)
            if isinstance(vertex, str) and vertex in self.vertex_config.vertex_set:
                v = merge_doc_basis(
                    v,
                    tuple(self.vertex_config.index(vertex).fields),
                    DISCRIMINANT_KEY
                    if self.vertex_config.discriminant_chart[vertex]
                    else None,
                )
            unit[vertex] = v

        unit = add_blank_collections(unit, self.vertex_config)

        unit = apply_filter(unit, vertex_conf=self.vertex_config)
        ctx.acc = unit
        return ctx


_NodeTypePriority: MappingProxyType[Type[Actor], int] = MappingProxyType(
    {
        DescendActor: 10,
        TransformActor: 20,
        VertexActor: 50,
        NormalizerActor: 70,
        EdgeActor: 90,
    }
)


class ActorWrapper:
    def __init__(self, *args, **kwargs):
        self.actor: Actor
        if self._try_init_descend(*args, **kwargs):
            pass
        elif self._try_init_transform(**kwargs):
            pass
        elif self._try_init_vertex(**kwargs):
            pass
        elif self._try_init_edge(**kwargs):
            pass
        elif self._try_init_normalizer(**kwargs):
            pass
        else:
            raise ValueError(f"Not able to init ActionNodeWrapper with {kwargs}")

    def finish_init(self, **kwargs):
        kwargs["transforms"] = kwargs.get("transforms", {})
        self.vertex_config = kwargs.get("vertex_config", VertexConfig(vertices=[]))
        kwargs["vertex_config"] = self.vertex_config
        self.edge_config = kwargs.get("edge_config", EdgeConfig())
        kwargs["edge_config"] = self.edge_config
        self.actor.finish_init(**kwargs)

    def _try_init_descend(self, *args, **kwargs) -> bool:
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
        self.actor = DescendActor(descend_key, descendants_kwargs=descendants, **kwargs)
        return True

    def _try_init_transform(self, **kwargs) -> bool:
        try:
            self.actor = TransformActor(**kwargs)
            return True
        except Exception:
            return False

    def _try_init_normalizer(self, **kwargs) -> bool:
        try:
            self.actor = NormalizerActor(**kwargs)
            return True
        except Exception:
            return False

    def _try_init_vertex(self, **kwargs) -> bool:
        try:
            self.actor = VertexActor(**kwargs)
            return True
        except Exception:
            return False

    def _try_init_edge(self, **kwargs) -> bool:
        try:
            self.actor = EdgeActor(**kwargs)
            return True
        except Exception:
            return False

    def __call__(self, ctx: ActionContext, *nargs, **kwargs) -> ActionContext:
        ctx = self.actor(ctx, *nargs, **kwargs)
        return ctx

    def normalize_unit(
        self, ctx: ActionContext, edges: list[Edge]
    ) -> defaultdict[GraphEntity, list]:
        unit = ctx.acc

        for vertex, v in unit.items():
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
            unit[vertex] = v

        unit = add_blank_collections(unit, self.vertex_config)

        unit = apply_filter(unit, vertex_conf=self.vertex_config)

        # pure_weight = extract_weights(unit_doc, edge_config.edges)

        unit = define_edges(
            unit=unit,
            unit_weights=defaultdict(),
            current_edges=edges,
            vertex_conf=self.vertex_config,
        )

        return unit

    @classmethod
    def from_dict(cls, data: dict | list):
        if isinstance(data, list):
            return cls(*data)
        else:
            return cls(**data)
