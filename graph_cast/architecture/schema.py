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

        self._current_resource: None | Resource = None
        pass

    def select_resource(self, name: str):
        for r in self.resources.tree_likes:
            if r.name == name:
                self._current_resource = r

        for r in self.resources.row_likes:
            if r.name == name:
                self._current_resource = r

    @property
    def current_resource(self):
        assert self._current_resource is not None
        return self._current_resource

    """
    -   how: all
        source:
            name: publication
            _anchor: main
            fields:
            -   _anchor
        target:
            name: date
            _anchor: main

    __OR__

    -   type: edge
    how: all
    source:
        name: mention
        _anchor: triple_index
    target:
        name: mention
        _anchor: core
        fields:
        -   _role
    index:
    -   fields:
        -   _role
    """
