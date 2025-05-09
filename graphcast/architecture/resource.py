import dataclasses
import logging
from collections import defaultdict
from typing import Callable

from dataclass_wizard import JSONWizard

from graphcast.architecture.actors import (
    ActionContext,
    ActorWrapper,
)
from graphcast.architecture.edge import Edge, EdgeConfig
from graphcast.architecture.onto import (
    EncodingType,
    GraphEntity,
)
from graphcast.architecture.transform import Transform
from graphcast.architecture.vertex import (
    VertexConfig,
    VertexRepresentationHelper,
)
from graphcast.onto import BaseDataclass

logger = logging.getLogger(__name__)


@dataclasses.dataclass(kw_only=True)
class Resource(BaseDataclass, JSONWizard):
    resource_name: str
    apply: list
    encoding: EncodingType = EncodingType.UTF_8
    merge_collections: list[str] = dataclasses.field(default_factory=list)
    extra_weights: list[Edge] = dataclasses.field(default_factory=list)
    types: dict[str, str] = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        self.root = ActorWrapper(*self.apply)
        self.vertex_rep: dict[str, VertexRepresentationHelper] = dict()
        self._types: dict[str, Callable] = dict()
        self.vertex_config: VertexConfig
        self.edge_config: EdgeConfig
        for k, v in self.types.items():
            try:
                self._types[k] = eval(v)
            except Exception as ex:
                logger.error(
                    f"For resource {self.name} for field {k} failed to cast type {v} : {ex}"
                )

    @property
    def name(self):
        return self.resource_name

    def finish_init(
        self,
        vertex_config: VertexConfig,
        edge_config: EdgeConfig,
        transforms: dict[str, Transform],
    ):
        self.vertex_config = vertex_config
        self.edge_config = edge_config
        self.root.finish_init(
            vertex_config=vertex_config,
            vertex_rep=self.vertex_rep,
            transforms=transforms,
        )
        for e in self.extra_weights:
            e.finish_init(vertex_config)

    def __call__(self, doc: dict) -> defaultdict[GraphEntity, list]:
        ctx = ActionContext()
        ctx = self.root(ctx, doc=doc)
        acc = self.root.normalize_unit(ctx, self.edge_config.edges)

        return acc

    def count(self):
        return self.root.count()
