from __future__ import annotations

import logging
from typing import TypeVar

from graph_cast.architecture.graph import GraphConfig
from graph_cast.architecture.util import strip_prefix
from graph_cast.architecture.vertex import VertexConfig

logger = logging.getLogger(__name__)

ConfiguratorType = TypeVar("ConfiguratorType", bound="Configurator")


class Configurator:
    def __init__(self, config):
        config = strip_prefix(config)
        general = config.get("general", {})
        self.name = general.get("name", "dummy")
        self.vertex_config = VertexConfig(config["vertex_collections"])
        edge_collections = config.get("edge_collections", ())
        self.graph_config = GraphConfig(edge_collections, self.vertex_config)
        self.current_fname: str | None = None
        self.merge_collections = tuple()

    @property
    def encoding(self):
        return "utf-8"

    @property
    def current_edges(self):
        return []

    @property
    def current_collections(self):
        return []

    @property
    def current_transform_config(self):
        return []

    def graph(self, u, v, ix=0):
        return self.graph_config.graph(u, v, ix)

    def exclude_fields(self, k):
        return self.graph_config.exclude_fields(k)

    # def transform_config(self):
    #     pass
