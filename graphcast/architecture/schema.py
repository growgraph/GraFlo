import dataclasses
import logging
from typing import Optional

from graphcast.architecture.edge import EdgeConfig
from graphcast.architecture.resource import Resource
from graphcast.architecture.transform import Transform
from graphcast.architecture.vertex import VertexConfig
from graphcast.onto import BaseDataclass

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SchemaMetadata(BaseDataclass):
    name: str


@dataclasses.dataclass
class Schema(BaseDataclass):
    general: SchemaMetadata
    vertex_config: VertexConfig
    edge_config: EdgeConfig
    resources: list[Resource]
    transforms: dict[str, Transform] = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        self.edge_config.finish_init(self.vertex_config)
        for r in self.resources:
            r.finish_init(
                vertex_config=self.vertex_config,
                edge_config=self.edge_config,
                transforms=self.transforms,
            )

    def fetch_resource(self, name: Optional[str] = None) -> Resource:
        _current_resource = None

        if name is not None:
            try:
                _current_resource = next(r for r in self.resources if r.name == name)
            except StopIteration:
                raise ValueError(f"Resource {name} not found")
        if self.resources and _current_resource is None:
            _current_resource = self.resources[0]
        else:
            raise ValueError(f"Resource {name} not found")
        return _current_resource
