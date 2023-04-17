from __future__ import annotations

import dataclasses
import logging
from abc import ABC
from copy import deepcopy
from enum import Enum
from typing import Union

from dataclass_wizard import JSONWizard

from graph_cast.architecture.transform import Transform

logger = logging.getLogger(__name__)

_anchor_key = "anchor"
_source_aux = "__source"
_target_aux = "__target"


# type for vertex or edge name (index)
TypeVE = Union[str, tuple[str, str]]


@dataclasses.dataclass
class ABCFields(ABC):
    collection_name: str | None = None
    fields: list[str] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class WeightConfig(ABCFields, JSONWizard):
    mapper: dict | None = None
    condition: dict | None = None


@dataclasses.dataclass
class CollectionIndex(ABCFields, JSONWizard):
    unique: bool = True
    type: str = "persistent"
    deduplicate: bool = True
    sparse: bool = False

    def __post_init__(self):
        if not self.fields:
            self.fields = ["_key"]

    def __iter__(self):
        return iter(self.fields)


class Vertex:
    def __init__(
        self,
        name,
        basename=None,
        index=(),
        fields=(),
        extra_index=(),
        numeric_fields=(),
        filters=(),
        transforms=(),
    ):
        self._name = name
        self._dbname = name if basename is None else basename
        self._fields = list(fields)
        self._index: CollectionIndex = CollectionIndex(fields=index)
        self._extra_indices: list[CollectionIndex] | None = (
            None
            if extra_index is None
            else [CollectionIndex(**item) for item in extra_index]
        )
        self._numeric_fields = numeric_fields
        # set of filters
        self._filters = [Filter(**item) for item in filters]

        # currently not used
        self._transforms = [Transform(**item) for item in transforms]

    @property
    def dbname(self):
        return self._dbname

    @property
    def fields(self):
        return self._fields

    @property
    def name(self):
        return self._name

    @property
    def index(self):
        return self._index

    @property
    def extra_indices(self) -> list[CollectionIndex]:
        return [] if self._extra_indices is None else self._extra_indices

    @property
    def numeric_fields(self):
        return self._numeric_fields

    @property
    def filters(self):
        return self._filters


@dataclasses.dataclass
class VertexHelper(JSONWizard):
    name: str
    field: str | bool = False
    anchor: str | bool = False
    weight_exclusive: list[str] = dataclasses.field(default_factory=lambda: [])
    fields: list[str] = dataclasses.field(default_factory=lambda: [])


class Edge:
    def __init__(self, dictlike, vconf: VertexConfig, direct=True):
        self._source: VertexHelper
        self._target: VertexHelper
        self._weight: list[str] = []
        self._weight_vertices: list[WeightConfig] = []
        self._weight_dict: dict = dict()
        self._extra_indices: list[CollectionIndex] = []
        self._how: EdgeMapping = dictlike.pop("how", EdgeMapping.ALL)
        self._type = "direct" if direct else "indirect"
        self._by = None

        try:
            if isinstance(dictlike["source"], dict) and isinstance(
                dictlike["target"], dict
            ):
                # dictlike["source"] is a dict with a spec of source
                self._init_local_definition(dictlike)
            else:
                # dictlike["source"] is a string with the name of the collection
                self._init_basic(dictlike)
        except KeyError as e:
            raise KeyError(
                f" source of target missing in edge definition :{e}"
            )

        self._init_indices(dictlike, vconf)

        if "weight" in dictlike:
            # legacy hack for when dictlike["weight"] contained only a mapper / dict
            self._weight_vertices = []
            for item in dictlike["weight"]:
                try:
                    self._weight_vertices += [WeightConfig(**item)]
                except:
                    logger.error(
                        "_weight_collections init failed for edge"
                        f" {self.edge_name_dyad}"
                    )
            if isinstance(dictlike["weight"], dict):
                if "fields" in dictlike["weight"]:
                    self._weight = dictlike["weight"]["fields"]
                elif "vertex" not in dictlike["weight"]:
                    # neither `fields` nor `vertex` is in dictlike["weight"]
                    self._weight_dict = dictlike["weight"]
            elif isinstance(dictlike["weight"], list):
                self._weight = dictlike["weight"]
            elif isinstance(dictlike["weight"], str):
                self._weight = [dictlike["weight"]]

        if self._type == "indirect" and "by" in dictlike:
            self._by = vconf.vertex_dbname(dictlike["by"])

        self._source_collection = vconf.vertex_dbname(self.source)
        self._target_collection = vconf.vertex_dbname(self.target)

        self._edge_name = f"{vconf.vertex_dbname(self.source)}_{vconf.vertex_dbname(self.target)}_edges"
        self._graph_name = f"{vconf.vertex_dbname(self.source)}_{vconf.vertex_dbname(self.target)}_graph"

    @property
    def source(self):
        return self._source.name

    @property
    def target(self):
        return self._target.name

    def update_weights(self, edge_with_weights):
        self._weight = edge_with_weights.weight_fields
        self._weight_dict = edge_with_weights.weight_dict
        self._weight_vertices = edge_with_weights.weight_vertices

    def _init_basic(self, dictlike):
        dictlike = strip_prefix(dictlike)
        self._source = VertexHelper(name=dictlike["source"])
        self._target = VertexHelper(name=dictlike["target"])

    def _init_local_definition(self, dictlike):
        """
        used for input/json
        :param dictlike:
        :return:
        """
        dictlike = strip_prefix(dictlike)

        self._source = VertexHelper(**dictlike["source"])
        self._target = VertexHelper(**dictlike["target"])

    def _init_indices(self, dictlike, vconf):
        """
        index should be consistent with weight
        :param dictlike:
        :param vconf:
        :return:
        """
        if "index" in dictlike:
            for item in dictlike["index"]:
                self._extra_indices += [self._init_index(item, vconf)]
            self._extra_indices = [
                x for x in self._extra_indices if x is not None
            ]

    def _init_index(self, item, vconf: VertexConfig) -> CollectionIndex | None:
        """
        default behavior for edge indices : add ["_from", "_to"] for uniqueness
        :param item:
        :param vconf:
        :return:
        """
        item = deepcopy(item)
        collection_name = item.get("collection_name", None)

        fields = item.pop("fields", [])
        index_fields = []
        if collection_name is not None:
            if not fields:
                fields = vconf.index(collection_name).fields
            index_fields += [f"{collection_name}.{x}" for x in fields]
        else:
            index_fields += fields

        unique = item.pop("unique", True)
        index_type = item.pop("type", "persistent")
        deduplicate = item.pop("deduplicate", True)
        sparse = item.pop("sparse", False)

        if index_fields:
            index_fields = ["_from", "_to"] + index_fields
            return CollectionIndex(
                collection_name=collection_name,
                fields=index_fields,
                unique=unique,
                type=index_type,
                deduplicate=deduplicate,
                sparse=sparse,
            )
        else:
            return None

    @property
    def source_exclude(self):
        return [self._source.field]

    @property
    def target_exclude(self):
        return [self._target.field]

    @property
    def edge_name_dyad(self):
        return self.source, self.target

    @property
    def source_collection(self):
        return self._source_collection

    @property
    def target_collection(self):
        return self._target_collection

    @property
    def edge_name(self):
        return self._edge_name

    @property
    def how(self):
        return self._how

    @property
    def graph_name(self):
        return self._graph_name

    @property
    def weight_fields(self):
        return () if self._weight is None else self._weight

    @property
    def weight_dict(self):
        return dict() if self._weight_dict is None else self._weight_dict

    @property
    def weight_vertices(self) -> list[WeightConfig]:
        return [] if self._weight_vertices is None else self._weight_vertices

    @property
    def type(self):
        return self._type

    @property
    def by(self):
        return self._by

    @property
    def index(self):
        return self._extra_indices

    def __iadd__(self, other: Edge):
        if self.edge_name_dyad == other.edge_name_dyad:
            self._extra_indices += other._extra_indices
            self._weight += other._weight
            self._weight_vertices += other._weight_vertices
            self._weight_dict.update(other._weight_dict)
            # self._source_exclude += other._source_exclude
            # self._target_exclude += other._target_exclude
            # self._how = dictlike.pop("how", None)
            # self._type = "direct" if direct else "indirect"
            # self._by = None
            return self
        else:
            raise ValueError(
                "can only update Edge definitions of the same type"
            )


class Filter:
    def __init__(self, b, a=None):
        """
        for a given doc it's a(doc) => b(doc) implication
        `a` and `b` are conditions. Return `False` means `doc` should be filtered out.
        if `doc` satisfies `a` condition then return the result of condition `b`
        if `doc` satisfies `a` condition then return True (not filtered)
        `a` is None condition then return the result of condition `b`
        :param b:
        :param a:
        """
        self.a = Condition(**a)
        self.b = Condition(**b)

    def __call__(self, doc):
        if self.a is not None:
            if self.a(**doc):
                return self.b(**doc)
            else:
                return True
        else:
            return self.b(**doc)

    def __str__(self):
        return f"{self.__class__} | a: {self.a} b: {self.b}"

    __repr__ = __str__


class Condition:
    def __init__(self, field, foo, value=None):
        self.field = field
        self.value = value
        # self.foo = getattr(self.value, foo)
        self.foo = foo

    def __call__(self, **kwargs):
        if self.field in kwargs:
            foo = getattr(kwargs[self.field], self.foo)
            return foo(self.value)
        else:
            return True

    def __str__(self):
        return (
            f"{self.__class__} | field: {self.field} value: {self.value} ->"
            f" foo: {self.foo}"
        )

    __repr__ = __str__


class VertexConfig:
    def __init__(self, vconfig):
        self._vcollections_all: dict[str, Vertex] = {}

        self._vcollections = set()

        # vertex_collection_name -> [numeric fields]
        self._vcollection_numeric_fields_map = {}

        # list of blank collections
        self._blank_collections = set()

        # TODO introduce meaningful error in case `collections` key is absent
        config = vconfig["collections"]

        self._init_vcollections(config)
        self._init_names(config)

        self._init_numeric_fields(config)
        if "blanks" in vconfig:
            self._init_blank_collections(vconfig["blanks"])

    @property
    def collections(self):
        return self._vcollections

    def _init_vcollections(self, vconfig):
        self._vcollections = set(vconfig.keys())
        self._vcollections_all = {
            k: Vertex(name=k, **v) for k, v in vconfig.items()
        }

    def _init_names(self, vconfig):
        try:
            self._vmap = {
                k: v["basename"] for k, v in vconfig.items() if "basename" in v
            }
        except:
            raise KeyError(
                "vconfig does not have 'basename' for one of the vertex"
                " collections!"
            )

    def vertex_dbname(self, vertex_name):
        return self._vcollections_all[vertex_name].dbname

    def index(self, vertex_name):
        return self._vcollections_all[vertex_name].index

    def extra_index_list(self, vertex_name) -> list[CollectionIndex]:
        return self._vcollections_all[vertex_name].extra_indices

    def _init_blank_collections(self, vconfig):
        self._blank_collections = set(vconfig)
        if set(self._blank_collections) - set(self._vcollections):
            raise ValueError(
                f" Blank collections {self.blank_collections} are not defined"
                " as vertex collections"
            )

    @property
    def blank_collections(self):
        return iter(self._blank_collections)

    def fields(self, vertex_name):
        return self._vcollections_all[vertex_name].fields

    def _init_numeric_fields(self, vconfig):
        self._vcollection_numeric_fields_map = {
            k: v["numeric_fields"]
            for k, v in vconfig.items()
            if "numeric_fields" in v
        }

    def numeric_fields_list(self, vertex_name):
        if vertex_name in self._vcollections:
            if vertex_name in self._vcollection_numeric_fields_map:
                return self._vcollection_numeric_fields_map[vertex_name]
            else:
                return ()
        else:
            raise ValueError(
                " Accessing vertex collection numeric fields: vertex"
                f" collection {vertex_name} was not defined in config"
            )

    def filters(self):
        return (
            (vcol, f)
            for vcol, item in self._vcollections_all.items()
            for f in item.filters
        )


class EdgeMapping(str, Enum):
    ALL = "all"
    ONE_N = "1-n"


def strip_prefix(dictlike, prefix="~"):
    new_dictlike = {}
    if isinstance(dictlike, dict):
        for k, v in dictlike.items():
            if isinstance(k, str):
                k = k.lstrip(prefix)
            new_dictlike[k] = strip_prefix(v, prefix)
    elif isinstance(dictlike, list):
        return [strip_prefix(x) for x in dictlike]
    else:
        return dictlike
    return new_dictlike
