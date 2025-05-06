from __future__ import annotations

import dataclasses
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from types import MappingProxyType
from typing import Optional

from graphcast.architecture.edge import Edge
from graphcast.architecture.onto import (
    DISCRIMINANT_KEY,
    GraphEntity,
)
from graphcast.architecture.transform import Transform
from graphcast.architecture.vertex import (
    VertexConfig,
)
from graphcast.onto import BaseDataclass

logger = logging.getLogger(__name__)


DESCEND_KEY_VALUES = {"key"}
DUMMY_KEY = "__dummy_key__"
DRESSING_TRANSFORMED_VALUE_KEY = "__value__"

# ActionContext = tuple[dict | list, defaultdict[str, GraphEntity]]
# ActionContextPure = tuple[dict, defaultdict[str, GraphEntity]]
#


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


class VertexNode(ActionNode):
    def __init__(
        self,
        vertex: str,
        discriminant: Optional[str] = None,
        keep_fields: Optional[tuple[str]] = None,
    ):
        self.name = vertex
        self.discriminant: Optional[str] = discriminant
        self.keep_fields: Optional[tuple[str]] = keep_fields

        # vertex_rep[self.name] = VertexRepresentationHelper(
        #     name=self.name, fields=vc.fields(self.name)
        # )
        # if self.map:
        #     vertex_rep[self.name].maps += [dict(self.map)]
        # for t in self.transforms:
        #     vertex_rep[self.name].transforms += [(t.input, t.output)]

    def __call__(self, ctx: ActionContextPure, **kwargs):
        # take relevant fields from doc if available, otherwise try DRESSING_TRANSFORMED_VALUE_KEY

        vertex_config: Optional[VertexConfig] = kwargs.get("vertex_config", None)
        if not isinstance(vertex_config, VertexConfig):
            raise ValueError("vertex_config :VertexConfig is required")

        vertex_keys = vertex_config.fields(self.name)

        cdoc: dict
        if self.name in ctx.vertex_buffer:
            cdoc = ctx.vertex_buffer[self.name]
            cdoc.update({k: v for k, v in ctx.cdoc.items() if k not in cdoc})
            del ctx.vertex_buffer[self.name]
        else:
            cdoc = ctx.cdoc

        cdoc.update(
            {k: v for k, v in ctx.doc.items() if k not in cdoc and k in vertex_keys}
        )

        _doc = {k: cdoc[k] for k in vertex_keys if k in cdoc}
        if self.discriminant is not None:
            _doc.update({DISCRIMINANT_KEY: self.discriminant})
        if self.keep_fields is not None:
            _doc.update({f: ctx.doc[f] for f in self.keep_fields if f in ctx.doc})
        ctx.acc[self.name] += [_doc]
        return ctx


class TransformNode(ActionNode):
    def __init__(self, **kwargs):
        self.vertex: Optional[str] = kwargs.pop("target_vertex", None)
        self.t = Transform(**kwargs)

    def __call__(self, ctx: ActionContextPure, **kwargs):
        _update_doc: dict
        if isinstance(ctx.doc, dict):
            _update_doc = self.t(ctx.doc, __return_doc=True, **kwargs)
        else:
            value = self.t(ctx.doc, __return_doc=False, **kwargs)
            _update_doc = {DRESSING_TRANSFORMED_VALUE_KEY: value}
        if self.vertex is None:
            ctx.cdoc.update(_update_doc)
        else:
            ctx.vertex_buffer[self.vertex] = _update_doc
        return ctx


class DescendNode(ActionNode):
    def __init__(self, key: Optional[str], descendants_kwargs: list):
        self.key = key
        self.descendants: list[ActionNodeWrapper] = []
        for descendant_kwargs in descendants_kwargs:
            self.descendants += [ActionNodeWrapper(**descendant_kwargs)]

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

    def __call__(self, ctx: ActionContext, **kwargs):
        if isinstance(ctx.doc, dict) and self.key in ctx.doc:
            ctx.doc = ctx.doc[self.key]
        for anw in self.descendants:
            ctx = anw(ctx, **kwargs)
        return ctx


# class DressNode(ActionNode):
#     def __init__(self, **kwargs):
#         pass
#
#     def __call__(self, ctx: ActionContext, **kwargs):
#         doc = {DUMMY_KEY: doc}
#         return doc, acc


_NodeTypePriority = MappingProxyType(
    {
        # DressNode: 10,
        TransformNode: 20,
        VertexNode: 50,
        DescendNode: 60,
        Edge: 90,
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
        elif self._try_init_dress_node(**kwargs):
            pass
        else:
            raise ValueError(f"Not able to init ActionNodeWrapper with {kwargs}")

    def _try_init_descend_node(self, *args, **kwargs) -> bool:
        descend_key_candidates = [kwargs.get(k, None) for k in DESCEND_KEY_VALUES]
        descend_key_candidates = [x for x in descend_key_candidates if x is not None]
        descend_key = descend_key_candidates[0] if descend_key_candidates else None
        ds = kwargs.get("apply", None)
        if ds is not None:
            if isinstance(ds, list):
                descendants = ds
            else:
                descendants = [ds]
        elif len(args) > 0:
            descendants = list(args)
        else:
            return False
        action_node = DescendNode(descend_key, descendants_kwargs=descendants)
        self.action_node = action_node
        return True

    def _try_init_transform_node(self, **kwargs) -> bool:
        try:
            action_node = TransformNode(**kwargs)
            self.action_node = action_node
            return True
        except Exception:
            return False

    def _try_init_vertex_node(self, **kwargs) -> bool:
        try:
            action_node = VertexNode(**kwargs)
            self.action_node = action_node
            return True
        except Exception:
            return False

    def _try_init_edge_node(self, **kwargs) -> bool:
        try:
            action_node = Edge(**kwargs)
            self.action_node = action_node
            return True
        except Exception:
            return False

    def _try_init_dress_node(self, **kwargs) -> bool:
        try:
            # action_node = DressNode(**kwargs)
            # self.action_node = action_node
            return True
        except Exception:
            return False

    def __call__(
        self,
        ctx: ActionContext,
        vertex_config: VertexConfig,
    ) -> ActionContext:
        if isinstance(ctx.doc, list):
            for sub_doc in ctx.doc:
                ctx.doc = sub_doc
                ctx.cdoc = {}
                ctx = self(ctx, vertex_config)
        else:
            kwargs = {"vertex_config": vertex_config}
            if isinstance(self.action_node, (VertexNode, DescendNode)):
                ctx = self.action_node(ctx, **kwargs)
            else:
                ctx = self.action_node(ctx)
        return ctx
