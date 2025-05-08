from collections import defaultdict

from graphcast.architecture import Edge, EdgeConfig, VertexConfig
from graphcast.architecture.actors import (
    DESCEND_KEY_VALUES,
    ActionContext,
    Actor,
    DescendActor,
    EdgeActor,
    TransformActor,
    VertexActor,
)
from graphcast.architecture.onto import DISCRIMINANT_KEY, GraphEntity
from graphcast.architecture.resource_util import (
    add_blank_collections,
    apply_filter,
    define_edges,
)
from graphcast.util.merge import merge_doc_basis
from graphcast.util.transform import pick_unique_dict


class ActorWrapper:
    def __init__(self, *args, **kwargs):
        self.action_node: Actor
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
        kwargs["vertex_config"] = self.vertex_config
        self.edge_config = kwargs.get("edge_config", EdgeConfig())
        kwargs["edge_config"] = self.edge_config
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
        self.action_node = DescendActor(
            descend_key, descendants_kwargs=descendants, **kwargs
        )
        return True

    def _try_init_transform_node(self, **kwargs) -> bool:
        try:
            self.action_node = TransformActor(**kwargs)
            return True
        except Exception:
            return False

    def _try_init_vertex_node(self, **kwargs) -> bool:
        try:
            self.action_node = VertexActor(**kwargs)
            return True
        except Exception:
            return False

    def _try_init_edge_node(self, **kwargs) -> bool:
        try:
            self.action_node = EdgeActor(**kwargs)
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
