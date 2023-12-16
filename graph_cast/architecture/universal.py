from __future__ import annotations

import logging
from enum import Enum
from typing import TypeVar

from graph_cast.architecture.graph import GraphConfig
from graph_cast.architecture.schema import VertexConfig
from graph_cast.architecture.util import strip_prefix
from graph_cast.onto import BaseDataclass

logger = logging.getLogger(__name__)


class Schema:
    def __init__(self, config):
        config = strip_prefix(config)
        general = config.get("schema", {})
        self.name = general.get("name", "dummy")
        self.vertex_config = VertexConfig(config["vertex_collections"])
        edge_collections = config.get("edge_collections", ())
        self.graph_config = GraphConfig(edge_collections, self.vertex_config)
        self.current_fname: str | None = None
        self.merge_collections = tuple()
