from __future__ import annotations

import dataclasses
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from itertools import product
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Iterable, Optional, Type, TypeVar

from graphcast.architecture.edge import Edge, EdgeConfig
from graphcast.architecture.onto import (
    SOURCE_AUX,
    TARGET_AUX,
    EdgeCastingType,
    GraphEntity,
)
from graphcast.architecture.resource_util import (
    add_blank_collections,
)
from graphcast.architecture.transform import Transform
from graphcast.architecture.util import project_dict
from graphcast.architecture.vertex import (
    VertexConfig,
)
from graphcast.onto import BaseDataclass
from graphcast.util.merge import merge_doc_basis
from graphcast.util.transform import pick_unique_dict

logger = logging.getLogger(__name__)


DESCEND_KEY_VALUES = {"key"}
DRESSING_TRANSFORMED_VALUE_KEY = "__value__"


def inner_factory_vertex() -> defaultdict[Optional[str], list]:
    return defaultdict(list)


def outer_factory() -> defaultdict[str, defaultdict[Optional[str], list]]:
    return defaultdict(inner_factory_vertex)


def dd_factory() -> defaultdict[GraphEntity, list]:
    return defaultdict(list)


@dataclasses.dataclass(kw_only=True)
class ActionContext(BaseDataclass):
    # accumulation of vertices at the local level
    # each local edge actors pushed current acc_vertex_local to acc_vectex
    acc_vertex_local: defaultdict[str, defaultdict[Optional[str], list]] = (
        dataclasses.field(default_factory=outer_factory)
    )
    acc_vertex: defaultdict[str, defaultdict[Optional[str], list]] = dataclasses.field(
        default_factory=outer_factory
    )
    acc: defaultdict[GraphEntity, list] = dataclasses.field(default_factory=dd_factory)

    vertex_buffer: defaultdict[GraphEntity, dict] = dataclasses.field(
        default_factory=lambda: defaultdict(dict)
    )
    # current doc : the result of application of transformations to the original document
    cdoc: dict = dataclasses.field(default_factory=dict)


class Actor(ABC):
    @abstractmethod
    def __call__(self, ctx: ActionContext, *nargs, **kwargs):
        pass

    def fetch_important_items(self):
        return {}

    def finish_init(self, **kwargs):
        pass

    def count(self):
        return 1

    def _filter_items(self, items):
        return {k: v for k, v in items.items() if v is not None and v}

    def _stringify_items(self, items):
        return {
            k: ", ".join(list(v)) if isinstance(v, (tuple, list)) else v
            for k, v in items.items()
        }

    def __str__(self):
        d = self.fetch_important_items()
        d = self._filter_items(d)
        d = self._stringify_items(d)
        d_list = [[k, d[k]] for k in sorted(d)]
        d_list_b = [type(self).__name__] + [": ".join(x) for x in d_list]
        d_list_str = "\n".join(d_list_b)
        return d_list_str

    __repr__ = __str__

    def fetch_actors(self, level, edges):
        return level, type(self), str(self), edges


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

    def fetch_important_items(self):
        sd = self.__dict__
        return {k: sd[k] for k in ["name", "discriminant", "keep_fields"]}

    def finish_init(self, **kwargs):
        self.vertex_config: VertexConfig = kwargs.pop("vertex_config")
        self.vertex_config.discriminant_chart[self.name] = True

    def __call__(self, ctx: ActionContext, *nargs, **kwargs):
        doc: dict = kwargs.pop("doc", {})

        # take relevant fields from doc if available, otherwise try DRESSING_TRANSFORMED_VALUE_KEY
        vertex_keys = self.vertex_config.fields(self.name)

        _doc: dict

        custom_transform = ctx.vertex_buffer.pop(self.name, {})

        # 1. exhaust custom_transform
        _doc = {k: custom_transform[k] for k in vertex_keys if k in custom_transform}

        # 2. exhaust cdoc
        n_value_keys = len(
            [k for k in ctx.cdoc if k.startswith(DRESSING_TRANSFORMED_VALUE_KEY)]
        )
        for j in range(n_value_keys):
            vkey = self.vertex_config.index(self.name).fields[j]
            v = ctx.cdoc.pop(f"{DRESSING_TRANSFORMED_VALUE_KEY}#{j}")
            _doc[vkey] = v

        for vkey in set(vertex_keys) - set(_doc):
            v = ctx.cdoc.pop(vkey, None)
            if v is not None:
                _doc[vkey] = v

        # 3. exhaust doc
        for vkey in set(vertex_keys) - set(_doc):
            v = doc.pop(vkey, None)
            if v is not None:
                _doc[vkey] = v

        # if self.keep_fields is not None:
        #     _doc.update({f: doc[f] for f in self.keep_fields if f in doc})
        if all(cfilter(doc) for cfilter in self.vertex_config.filters(self.name)):
            ctx.acc_vertex_local[self.name][self.discriminant] += [_doc]

        return ctx


class EdgeActor(Actor):
    def __init__(
        self,
        **kwargs,
    ):
        self.edge = Edge.from_dict(kwargs)
        self.vertex_config: VertexConfig

    def fetch_important_items(self):
        sd = self.edge.__dict__
        return {
            k: sd[k]
            for k in ["source", "target", "source_discriminant", "target_discriminant"]
        }

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

        # self.edge.source_discriminant
        # get source and target items
        source_items, target_items = (
            ctx.acc_vertex_local[source].pop(self.edge.source_discriminant, []),
            ctx.acc_vertex_local[target].pop(self.edge.target_discriminant, []),
        )
        source_items = [
            item for item in source_items if any(k in item for k in source_index)
        ]
        target_items = [
            item for item in target_items if any(k in item for k in target_index)
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
        ctx.acc[source, target, relation] += edges

        ctx.acc_vertex[source][self.edge.source_discriminant] += source_items
        ctx.acc_vertex[target][self.edge.target_discriminant] += target_items

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
        self.__init = kwargs
        self.vertex: Optional[str] = kwargs.pop("target_vertex", None)
        self.transforms: dict
        self.name = kwargs.get("name", None)
        self.t = Transform(**kwargs)

    def fetch_important_items(self):
        sd = self.__dict__
        sm = {k: sd[k] for k in ["name", "vertex"]}
        smb = {"t.input": self.t.input, "t.output": self.t.output}
        return {**sm, **smb}

    def finish_init(self, **kwargs):
        self.transforms = kwargs.pop("transforms", {})
        if self.name is not None:
            other = self.transforms.get(self.name, None)
            t_self, t_lib = self.t.get_barebone(other)
            if t_lib is not None:
                self.transforms[self.name] = t_lib
            if t_self is not None:
                self.t = t_self

    def __call__(self, ctx: ActionContext, *nargs, **kwargs):
        logging.debug(f"transforms : {id(self.transforms)} {len(self.transforms)}")

        if kwargs:
            doc: Optional[dict] = kwargs.get("doc")
        elif nargs:
            doc = nargs[0]
        else:
            raise ValueError(f"{type(self).__name__}: doc should be provided")

        if self.t.is_dummy and self.name is not None:
            t = self.transforms[self.name]
        else:
            t = self.t

        _update_doc: dict
        if isinstance(doc, dict):
            _update_doc = t(doc)
        else:
            value = t(doc)
            if isinstance(value, tuple):
                _update_doc = {
                    f"{DRESSING_TRANSFORMED_VALUE_KEY}#{j}": v
                    for j, v in enumerate(value)
                }
            elif isinstance(value, dict):
                _update_doc = value
            else:
                _update_doc = {f"{DRESSING_TRANSFORMED_VALUE_KEY}#0": value}

        if self.vertex is None:
            ctx.cdoc.update(_update_doc)
        else:
            # prepared for a specifique vertex
            # useful then two vertices have the same keys, e.g. `id`
            ctx.vertex_buffer[self.vertex] = _update_doc
        return ctx


class DescendActor(Actor):
    def __init__(self, key: Optional[str], descendants_kwargs: list, **kwargs):
        self.key = key
        self._descendants: list[ActorWrapper] = []
        for descendant_kwargs in descendants_kwargs:
            self._descendants += [ActorWrapper(**descendant_kwargs, **kwargs)]

    def fetch_important_items(self):
        sd = self.__dict__
        sm = {k: sd[k] for k in ["key"]}
        return {**sm}

    def add_descendant(self, d: ActorWrapper):
        self._descendants += [d]

    def count(self):
        return sum(d.count() for d in self.descendants)

    @property
    def descendants(self) -> list[ActorWrapper]:
        return sorted(self._descendants, key=lambda x: _NodeTypePriority[type(x.actor)])

    def finish_init(self, **kwargs):
        __add_normalizer = kwargs.get("__add_normalizer", True)

        self.vertex_config: VertexConfig = kwargs.get(
            "vertex_config", VertexConfig(vertices=[])
        )

        for an in self.descendants:
            an.finish_init(**kwargs)

        # autofill vertices
        # 1. check all transforms
        available_fields = set()
        for anw in self.descendants:
            actor = anw.actor
            if isinstance(actor, TransformActor):
                available_fields |= set(list(actor.t.output))

        # 2. find matching with vertex fields
        present_vertices = [
            anw.actor.name
            for anw in self.descendants
            if isinstance(anw.actor, VertexActor)
        ]

        # 3. adjust present vertices: remove fields from present vertices
        for v in present_vertices:
            available_fields -= set(self.vertex_config.fields(v))

        # 4. add vertices
        for v in self.vertex_config.vertices:
            intersection = available_fields & set(v.fields)
            if intersection and v.name not in present_vertices:
                new_descendant = ActorWrapper(vertex=v.name)
                new_descendant.finish_init(**kwargs)
                self.add_descendant(new_descendant)

        if __add_normalizer:
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

        if not doc:
            return ctx

        if self.key is not None:
            if isinstance(doc, dict) and self.key in doc:
                doc = doc[self.key]
            else:
                logging.error(f"doc {doc} was expected to have level {self.key}")
                return ctx

        doc_level = doc if isinstance(doc, list) else [doc]

        logger.debug(f"{len(doc_level)}")

        for i, sub_doc in enumerate(doc_level):
            logger.debug(f"docs: {i + 1}/{len(doc_level)}")
            if isinstance(sub_doc, dict):
                nargs: tuple = tuple()
                kwargs["doc"] = sub_doc
            else:
                # nargs deal with the case when the same property
                #   for a vertex class is provided as list of values
                # e.g. {"ids": ["abc", "abd", "qwe123"]}
                nargs = (sub_doc,)
            ctx.cdoc = {}

            for j, anw in enumerate(self.descendants):
                logger.debug(
                    f"{type(anw.actor).__name__}: {j + 1}/{len(self.descendants)}"
                )
                ctx = anw(ctx, *nargs, **kwargs)
        ctx.cdoc = {}
        return ctx

    def fetch_actors(self, level, edges):
        label_current = str(self)
        cname_current = type(self)
        hash_current = hash((level, cname_current, label_current))
        logger.info(f"{hash_current}, {level, cname_current, label_current}")
        props_current = {"label": label_current, "class": cname_current, "level": level}
        for d in self.descendants:
            level_a, cname, label_a, edges_a = d.fetch_actors(level + 1, edges)
            if cname == NormalizerActor:
                continue
            hash_a = hash((level_a, cname, label_a))
            props_a = {"label": label_a, "class": cname, "level": level_a}
            edges = [(hash_current, hash_a, props_current, props_a)] + edges_a
        return level, type(self), str(self), edges


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

        # for vertex, v in unit.items():
        #     v = pick_unique_dict(v)
        #     if isinstance(vertex, str) and vertex in self.vertex_config.vertex_set:
        #         v = merge_doc_basis(
        #             v,
        #             tuple(self.vertex_config.index(vertex).fields),
        #             DISCRIMINANT_KEY
        #             if self.vertex_config.discriminant_chart[vertex]
        #             else None,
        #         )
        #         for item in v:
        #             item.pop(DISCRIMINANT_KEY, None)
        #     unit[vertex] = v

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

    def count(self):
        return self.actor.count()

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
        for vertex, v in ctx.acc_vertex.items():
            for discriminant, vv in v.items():
                vv = pick_unique_dict(vv)
                vvv = merge_doc_basis(
                    vv,
                    tuple(self.vertex_config.index(vertex).fields),
                    discriminant_key=None,
                )

                ctx.acc[vertex] += vvv

        unit = ctx.acc

        unit = add_blank_collections(unit, self.vertex_config)

        # pure_weight = extract_weights(unit_doc, edge_config.edges)

        # unit = define_edges(
        #     unit=unit,
        #     unit_weights=defaultdict(),
        #     current_edges=edges,
        #     vertex_conf=self.vertex_config,
        # )

        return unit

    @classmethod
    def from_dict(cls, data: dict | list):
        if isinstance(data, list):
            return cls(*data)
        else:
            return cls(**data)

    def assemble_tree(self, fig_path: Optional[Path] = None):
        _, _, _, edges = self.fetch_actors(0, [])
        logger.info(f"{len(edges)}")
        try:
            import networkx as nx
        except ImportError as e:
            logger.error(f"not able to import networks {e}")
            return None
        nodes = {}
        g = nx.MultiDiGraph()
        for ha, hb, pa, pb in edges:
            nodes[ha] = pa
            nodes[hb] = pb
        from graphcast.plot.plotter import fillcolor_palette

        map_class2color = {
            NormalizerActor: "grey",
            DescendActor: fillcolor_palette["green"],
            VertexActor: "orange",
            EdgeActor: fillcolor_palette["violet"],
            TransformActor: fillcolor_palette["blue"],
        }

        for n, props in nodes.items():
            nodes[n]["fillcolor"] = map_class2color[props["class"]]
            nodes[n]["style"] = "filled"
            nodes[n]["color"] = "brown"

        edges = [(ha, hb) for ha, hb, _, _ in edges]
        g.add_edges_from(edges)
        g.add_nodes_from(nodes.items())

        if fig_path is not None:
            ag = nx.nx_agraph.to_agraph(g)
            ag.draw(
                fig_path,
                "pdf",
                prog="dot",
            )
            return None
        else:
            return g

    def fetch_actors(self, level, edges):
        return self.actor.fetch_actors(level, edges)
