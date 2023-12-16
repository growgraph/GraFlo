from __future__ import annotations

import dataclasses
import logging
from copy import deepcopy

from graph_cast.architecture.onto import (
    CollectionIndex,
    EdgeMapping,
    EdgeType,
    IndexType,
    VertexHelper,
    WeightConfig,
)
from graph_cast.architecture.transform import Transform
from graph_cast.architecture.util import strip_prefix
from graph_cast.onto import BaseDataclass, DBFlavor, Expression

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class Vertex(BaseDataclass):
    name: str
    fields: list[str]
    indexes: list[CollectionIndex] = dataclasses.field(default_factory=list)
    filters: list[Expression] = dataclasses.field(default_factory=list)
    transforms: list[Transform] = dataclasses.field(default_factory=list)
    dbname: str | None = None

    def __post_init__(self):
        if self.dbname is None:
            self.dbname = self.name
        union_fields = set(self.fields)
        for ei in self.indexes:
            union_fields |= set(ei.fields)
        self.fields = list(union_fields)


class Edge:
    def __init__(self, dictlike, vconf: VertexConfig, direct=True):
        self._source: VertexHelper
        self._target: VertexHelper
        self._weight: list[str] = []
        self._weight_vertices: list[WeightConfig] = []
        self._weight_dict: dict = dict()
        self._extra_indices: list[CollectionIndex] = []
        self._how: EdgeMapping = dictlike.pop("how", EdgeMapping.ALL)
        self._relation: str = dictlike.pop("relation", None)
        self._type: EdgeType = EdgeType.DIRECT if direct else EdgeType.INDIRECT
        self._by = None
        self._db_flavor = vconf.db_flavor

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
            for item in dictlike["weight"]:
                try:
                    self._weight_vertices += [WeightConfig(**item)]
                except:
                    logger.warning(
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

        if self.type == EdgeType.INDIRECT and "by" in dictlike:
            self._by = vconf.vertex_dbname(dictlike["by"])

        self._source_collection = vconf.vertex_dbname(self.source)
        self._target_collection = vconf.vertex_dbname(self.target)

        self._collection_name_suffix = dictlike.pop(
            "collection_name_suffix", ""
        )

        if self._collection_name_suffix:
            self._collection_name_suffix = f"{self._collection_name_suffix}_"

        self._source_dbname = vconf.vertex_dbname(self.source)
        self._target_dbname = vconf.vertex_dbname(self.target)

        self._graph_name = (
            f"{vconf.vertex_dbname(self.source)}_{vconf.vertex_dbname(self.target)}_"
            f"{self._collection_name_suffix}graph"
        )

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

    def _init_basic(self, dictlike: dict):
        dictlike_stripped: dict = strip_prefix(dictlike)
        self._source = VertexHelper(name=dictlike_stripped["source"])
        self._target = VertexHelper(name=dictlike_stripped["target"])

    def _init_local_definition(self, dictlike: dict):
        """
        used for input/json
        :param dictlike:
        :return:
        """
        dictlike_stripped: dict = strip_prefix(dictlike)

        self._source = VertexHelper(**dictlike_stripped["source"])
        self._target = VertexHelper(**dictlike_stripped["target"])

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
        collection_name = item.get("name", None)

        fields = item.pop("fields", [])
        index_fields = []
        if collection_name is not None:
            if not fields:
                fields = vconf.index(collection_name).fields
            index_fields += [f"{collection_name}@{x}" for x in fields]
        else:
            index_fields += fields

        unique = item.pop("unique", True)
        index_type: IndexType = item.pop("type", IndexType.PERSISTENT)
        deduplicate = item.pop("deduplicate", True)
        sparse = item.pop("sparse", False)
        fields_only = item.pop("fields_only", False)
        if index_fields:
            if not fields_only and self._db_flavor == DBFlavor.ARANGO:
                index_fields = ["_from", "_to"] + index_fields
            return CollectionIndex(
                name=collection_name,
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
        return [self._source.selector] if self._source.selector else []

    @property
    def target_exclude(self):
        return [self._target.selector] if self._target.selector else []

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
        if self._relation is None:
            if self._db_flavor == DBFlavor.ARANGO:
                x = f"{self._source_dbname}_{self._target_dbname}_{self._collection_name_suffix}edges"
            elif self._db_flavor == DBFlavor.NEO4J:
                x = f"{self._source_dbname}{self._target_dbname}"
            else:
                raise ValueError(f" Unknown DBFlavor: {self._db_flavor}")
        else:
            x = self._relation
        return x

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
    def type(self) -> EdgeType:
        return self._type

    @property
    def relation(self) -> str:
        if self._relation is None:
            return self.edge_name
        else:
            return self._relation

    @property
    def by(self):
        return self._by

    @property
    def indices(self) -> list[CollectionIndex]:
        return self._extra_indices

    @property
    def index(self) -> list[str]:
        return self._extra_indices[0].fields

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


@dataclasses.dataclass
class VertexConfig(BaseDataclass):
    collections: list[Vertex]
    blank_collections: list[str] = dataclasses.field(default_factory=list)
    db_flavor: DBFlavor = DBFlavor.ARANGO

    def __post_init__(self):
        self._vcollections_all: dict[str, Vertex] = {
            item.name: item for item in self.collections
        }
        self.collections_set = set(self._vcollections_all.keys())

        # TODO replace by types
        # vertex_collection_name -> [numeric fields]
        self._vcollection_numeric_fields_map = {}

        if set(self.blank_collections) - set(self.collections_set):
            raise ValueError(
                f" Blank collections {self.blank_collections} are not defined"
                " as vertex collections"
            )

    def vertex_dbname(self, vertex_name):
        try:
            value = self._vcollections_all[vertex_name].dbname
        except KeyError as e:
            logger.error(
                "Available vertex collections :"
                f" {self._vcollections_all.keys()}; vertex collection"
                f" requested : {vertex_name}"
            )
            raise e
        return value

    def index(self, vertex_name) -> CollectionIndex:
        return self._vcollections_all[vertex_name].indexes[0]

    def indexes(self, vertex_name) -> list[CollectionIndex]:
        return self._vcollections_all[vertex_name].indexes

    def fields(self, vertex_name: str):
        return self._vcollections_all[vertex_name].fields

    # def _init_numeric_fields(self, vconfig):
    #     self._vcollection_numeric_fields_map = {
    #         k: v["numeric_fields"]
    #         for k, v in vconfig.items()
    #         if "numeric_fields" in v
    #     }

    def numeric_fields_list(self, vertex_name):
        if vertex_name in self.collections_set:
            if vertex_name in self._vcollection_numeric_fields_map:
                return self._vcollection_numeric_fields_map[vertex_name]
            else:
                return ()
        else:
            raise ValueError(
                " Accessing vertex collection numeric fields: vertex"
                f" collection {vertex_name} was not defined in config"
            )

    def filters(self, vertex_name) -> list[Expression]:
        if vertex_name in self._vcollections_all:
            return self._vcollections_all[vertex_name].filters
        else:
            return []
