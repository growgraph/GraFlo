from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from types import MappingProxyType
from typing import Optional

from graphcast.architecture.edge import Edge
from graphcast.architecture.onto import (
    GraphEntity,
)
from graphcast.architecture.transform import Transform
from graphcast.architecture.vertex import (
    VertexConfig,
)

logger = logging.getLogger(__name__)


DESCEND_KEY_VALUES = {"key"}
DUMMY_KEY = "__dummy_key__"


ActionContext = tuple[dict | list, defaultdict[str, GraphEntity]]


class ActionNode(ABC):
    @abstractmethod
    def __call__(self, ctx: ActionContext, **kwargs):
        pass


class VertexNode(ActionNode):
    def __init__(self, vertex: str, discriminant: Optional[str] = None):
        self.name = vertex
        self.discriminant = discriminant

        # vertex_rep[self.name] = VertexRepresentationHelper(
        #     name=self.name, fields=vc.fields(self.name)
        # )
        # if self.map:
        #     vertex_rep[self.name].maps += [dict(self.map)]
        # for t in self.transforms:
        #     vertex_rep[self.name].transforms += [(t.input, t.output)]

    def __call__(self, ctx: ActionContext, **kwargs):
        _ = kwargs.get("vertex_config", None)

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
        doc, acc = ctx
        if isinstance(doc, dict) and self.key in doc:
            doc = doc[self.key]
        for anw in self.descendants:
            ctx = anw((doc, acc), **kwargs)
        return ctx


class DressNode(ActionNode):
    def __init__(self, **kwargs):
        pass

    def __call__(self, ctx: ActionContext, **kwargs):
        doc, acc = ctx
        doc = {DUMMY_KEY: doc}
        return doc, acc


_NodeTypePriority = MappingProxyType(
    {
        DressNode: 10,
        Transform: 20,
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
            action_node = Transform(**kwargs)
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
            action_node = DressNode(**kwargs)
            self.action_node = action_node
            return True
        except Exception:
            return False

    def __call__(
        self,
        ctx: ActionContext,
        vertex_config: VertexConfig,
    ) -> ActionContext:
        doc, acc = ctx
        if isinstance(doc, list):
            for sub_doc in doc:
                doc, acc = self((sub_doc, acc), vertex_config)
        else:
            doc, acc = self.action_node((doc, acc), vertex_config=vertex_config)

        return doc, acc
