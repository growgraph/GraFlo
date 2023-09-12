from __future__ import annotations

import abc
import logging
from collections import defaultdict
from collections.abc import Iterator
from enum import Enum
from typing import TypeVar

from graph_cast.architecture.graph import GraphConfig
from graph_cast.architecture.schema import VertexConfig, strip_prefix
from graph_cast.architecture.transform import TableMapper

logger = logging.getLogger(__name__)

ConfiguratorType = TypeVar("ConfiguratorType", bound="Configurator")


class DataSourceType(str, Enum):
    JSON = "json"
    TABLE = "table"


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

    @abc.abstractmethod
    def set_current_resource_name(self, resource):
        pass

    @property
    def encoding(self):
        return "utf-8"

    @property
    def current_graphs(self):
        return []

    @property
    def current_collections(self):
        return []

    @property
    def current_transformations(self):
        return []

    def graph(self, u, v, ix=0):
        return self.graph_config.graph(u, v, ix)

    def exclude_fields(self, k):
        return self.graph_config.exclude_fields(k)


class LocalVertexCollections:
    def __init__(self, inp):
        self._vcollections: defaultdict[str, list[TableMapper]] = defaultdict(
            list
        )
        for cc in inp:
            # TODO and type is allowed
            if "type" in cc:
                self._vcollections[cc["type"]] += [TableMapper(**cc)]

    def __iter__(self) -> Iterator[tuple[str, TableMapper]]:
        return (
            (k, m) for k in self.collections for m in self._vcollections[k]
        )

    @property
    def collections(self):
        return self._vcollections.keys()

    def update_mappers(self, **kwargs):
        for k, m in self:
            m.update(**kwargs)
