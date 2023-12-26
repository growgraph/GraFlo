from __future__ import annotations

import dataclasses
import logging

from graph_cast.architecture.edge import EdgeConfig
from graph_cast.architecture.resource import Resource, ResourceHolder
from graph_cast.architecture.vertex import VertexConfig
from graph_cast.onto import BaseDataclass

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SchemaMetadata(BaseDataclass):
    name: str


@dataclasses.dataclass
class Schema(BaseDataclass):
    general: SchemaMetadata
    vertex_config: VertexConfig
    edge_config: EdgeConfig
    resources: ResourceHolder

    def __post_init__(self):
        # add extra edges from tree resources?
        # set up edges wrt

        # 1. validate resources
        # 2 co-define edges from resources

        self.edge_config.finish_init(self.vertex_config)

        self.resources.finish_init(self.vertex_config, self.edge_config)
        pass

    def fetch_resource(self, name: str | None = None) -> Resource:
        _current_resource = None
        for r in self.resources.tree_likes:
            if name is None:
                _current_resource = r
            elif r.name == name:
                _current_resource = r

        for r in self.resources.row_likes:
            if name is None:
                _current_resource = r
            elif r.name == name:
                _current_resource = r
        if _current_resource is None:
            raise ValueError(f"Resource {name} not found")
        return _current_resource
