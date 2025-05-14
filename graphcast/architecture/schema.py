import dataclasses
import logging
from collections import Counter
from typing import Optional

from graphcast.architecture.edge import EdgeConfig
from graphcast.architecture.resource import Resource
from graphcast.architecture.transform import ProtoTransform
from graphcast.architecture.vertex import VertexConfig
from graphcast.onto import BaseDataclass

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SchemaMetadata(BaseDataclass):
    name: str
    version: Optional[str] = None


@dataclasses.dataclass
class Schema(BaseDataclass):
    general: SchemaMetadata
    vertex_config: VertexConfig
    edge_config: EdgeConfig
    resources: list[Resource]
    transforms: dict[str, ProtoTransform] = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        self.edge_config.finish_init(self.vertex_config)

        for r in self.resources:
            r.finish_init(
                vertex_config=self.vertex_config,
                edge_config=self.edge_config,
                transforms=self.transforms,
            )

        names = [r.name for r in self.resources]
        c = Counter(names)
        for k, v in c.items():
            if v > 1:
                raise ValueError(f"resource name {k} used {v} times")
        self._resources: dict[str, Resource] = {}
        for r in self.resources:
            self._resources[r.name] = r

    def fetch_resource(self, name: Optional[str] = None) -> Resource:
        _current_resource = None

        if name is not None:
            if name in self._resources:
                _current_resource = self._resources[name]
            else:
                raise ValueError(f"Resource {name} not found")
        else:
            if self._resources:
                _current_resource = self.resources[0]
            else:
                raise ValueError("Empty resource container 😕")
        return _current_resource
