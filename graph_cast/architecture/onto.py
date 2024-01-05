from __future__ import annotations

import dataclasses
import logging
from abc import ABCMeta
from collections import defaultdict
from typing import Any, Union

from graph_cast.onto import BaseDataclass, BaseEnum, DBFlavor
from graph_cast.util.transform import pick_unique_dict

ANCHOR_KEY = "_anchor"
SOURCE_AUX = "__source"
TARGET_AUX = "__target"

# type for vertex or edge name (index)
TypeVE = Union[str, tuple[str, str]]

# type for vertex or edge name (index)
GraphEntity = Union[str, tuple[str, str, str | None]]

logger = logging.getLogger(__name__)


class EdgeMapping(str, BaseEnum):
    ALL = "all"
    ONE_N = "1-n"


class EncodingType(str, BaseEnum):
    ISO_8859 = "ISO-8859-1"
    UTF_8 = "utf-8"


class IndexType(str, BaseEnum):
    PERSISTENT = "persistent"
    HASH = "hash"
    SKIPLIST = "skiplist"
    FULLTEXT = "fulltext"


class EdgeType(str, BaseEnum):
    """
    INDIRECT: defined as a collection, indexes are set up (possibly used after data ingestion)
    DIRECT : in addition to indexes, these edges are generated during ingestion
    """

    INDIRECT = "indirect"
    DIRECT = "direct"


@dataclasses.dataclass
class Field(BaseDataclass):
    name: str
    exclusive: bool = True


@dataclasses.dataclass
class ABCFields(BaseDataclass, metaclass=ABCMeta):
    name: str | None = None
    fields: list[str] = dataclasses.field(default_factory=list)

    def cfield(self, x):
        return f"{self.name}@{x}"


@dataclasses.dataclass
class Weight(ABCFields):
    map: dict = dataclasses.field(default_factory=dict)
    filter: dict = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class VertexHelper(ABCFields):
    # pre-select only vertices with anchor value
    # the value of _anchor_key
    _anchor: str | bool = False

    # create edges between vertices that have the same value of selector field is key
    selector: str | bool = False


@dataclasses.dataclass
class Index(BaseDataclass):
    name: str | None = None
    fields: list[str] = dataclasses.field(default_factory=list)
    unique: bool = True
    type: IndexType = IndexType.PERSISTENT
    deduplicate: bool = True
    sparse: bool = False
    exclude_edge_endpoints: bool = False

    def __iter__(self):
        return iter(self.fields)

    def db_form(self, db_type: DBFlavor):
        r = self.to_dict()
        if db_type == DBFlavor.ARANGO:
            _ = r.pop("name")
            _ = r.pop("exclude_edge_endpoints")
        elif db_type == DBFlavor.NEO4J:
            pass
        else:
            raise ValueError(f"Unknown db_type {db_type}")

        return r


class DataSourceType(str, BaseEnum):
    JSON = "json"
    TABLE = "csv"


class ItemsView:
    def __init__(self, gc: GraphContainer):
        self._dictlike = gc

    def __iter__(self):
        for key in self._dictlike.vertices:
            yield key, self._dictlike.vertices[key]
        for key in self._dictlike.edges:
            yield key, self._dictlike.edges[key]


@dataclasses.dataclass
class GraphContainer(BaseDataclass):
    vertices: dict[str, list]
    edges: dict[tuple[str, str, str | None], list]
    linear: list[defaultdict[str | tuple[str, str, str | None], list[Any]]]

    def __post_init__(self):
        pass

    def items(self):
        return ItemsView(self)

    def pick_unique(self):
        for k, v in self.vertices.items():
            self.vertices[k] = pick_unique_dict(v)
        for k, v in self.edges.items():
            self.edges[k] = pick_unique_dict(v)


def cast_graph_name_to_triple(s: GraphEntity):
    if isinstance(s, str):
        s2 = s.split("_")
        if len(s2) < 2:
            return s2[0]
        elif len(s2) == 2:
            return *s2[:-1], None
        elif len(s2) == 3:
            if s2[-1] in ["graph", "edges"]:
                return *s2[:-1], None
            else:
                return tuple(s2)
        elif len(s2) == 4 and s2[-1] in ["graph", "edges"]:
            return tuple(s2[:-1])
        raise ValueError(
            f"Invalid graph_name {s} : can not be cast to GraphEntity"
        )
    else:
        return s
