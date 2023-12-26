from __future__ import annotations

import dataclasses
import logging
from abc import ABCMeta
from typing import Union

from graph_cast.onto import BaseDataclass, BaseEnum

ANCHOR_KEY = "_anchor"
SOURCE_AUX = "__source"
TARGET_AUX = "__target"

# type for vertex or edge name (index)
TypeVE = Union[str, tuple[str, str]]

# type for vertex or edge name (index)
GraphEntity = Union[str, tuple[str, str], tuple[str, str, str | None]]

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
    exclude_edge_end_vertices: bool = False

    def __post_init__(self):
        if not self.fields:
            self.fields = ["_key"]

    def __iter__(self):
        return iter(self.fields)


class DataSourceType(str, BaseEnum):
    JSON = "json"
    TABLE = "csv"
