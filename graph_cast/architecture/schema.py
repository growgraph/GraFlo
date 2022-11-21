from __future__ import annotations

import dataclasses
from collections import defaultdict

from graph_cast.architecture.transform import Transform


@dataclasses.dataclass
class CollectionIndex:
    fields: list[str] = dataclasses.field(default_factory=list)
    unique: bool = True
    type: str = "hash"

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


class Edge:
    def __init__(self, dictlike, vconf: VertexConfig, direct=True):
        self._source_exclude: list[str] | None = None
        self._target_exclude: list[str] | None = None
        self._extra_indices: list[CollectionIndex] | None = None
        try:
            if isinstance(dictlike["source"], dict) and isinstance(
                dictlike["target"], dict
            ):
                self._init_local_definition(dictlike)
            else:
                self._init_basic(dictlike)
        except KeyError as e:
            raise KeyError(
                f" source of target missing in edge definition :{e}"
            )

        self._init_indices(dictlike, vconf)
        self._weight = dictlike["weight"] if "weight" in dictlike else None
        self._type = "direct" if direct else "indirect"
        self._by = None
        if self._type == "indirect" and "by" in dictlike:
            self._by = vconf.vertex_dbname(dictlike["by"])

        self._edge_name = f"{vconf.vertex_dbname(self.source)}_{vconf.vertex_dbname(self.target)}_edges"
        self._graph_name = f"{vconf.vertex_dbname(self.source)}_{vconf.vertex_dbname(self.target)}_graph"

    @property
    def edge_name(self):
        return self._edge_name

    @property
    def graph_name(self):
        return self._graph_name

    @property
    def weight(self):
        return dict() if self._weight is None else self._weight

    @property
    def type(self):
        return self._type

    @property
    def by(self):
        return self._by

    @property
    def index(self):
        return [] if self._extra_indices is None else self._extra_indices

    def _init_basic(self, dictlike):
        self.source = dictlike["source"]
        self.target = dictlike["target"]

    def _init_local_definition(self, dictlike):
        """
        used for input/json
        :param dictlike:
        :return:
        """
        self.source = dictlike["source"]["name"]
        self.target = dictlike["target"]["name"]
        self._source_exclude = (
            [dictlike["source"]["field"]]
            if "field" in dictlike["source"]
            else None
        )
        self._target_exclude = (
            [dictlike["target"]["field"]]
            if "field" in dictlike["target"]
            else None
        )

    def _init_indices(self, dictlike, vconf):
        """
        index should be consistent with weight
        :param dictlike:
        :param vconf:
        :return:
        """
        if "index" in dictlike:
            self._extra_indices = []
            for item in dictlike["index"]:
                self._extra_indices += [self._init_index(item, vconf)]
            self._extra_indices = [
                x for x in self._extra_indices if x is not None
            ]

    def _init_index(self, item, vconf: VertexConfig):
        if "fields" in item:
            return CollectionIndex(**item)
        elif "collection" in item:
            if item["collection"] in vconf.collections:
                unique = False
                cfields = vconf.index(item["collection"]).fields
                local_fields = [f"{item['collection']}.{x}" for x in cfields]
                return CollectionIndex(
                    fields=local_fields, unique=unique, type="hash"
                )
            else:
                return None

    @property
    def source_exclude(self):
        return [] if self._source_exclude is None else self._source_exclude

    @property
    def target_exclude(self):
        return [] if self._target_exclude is None else self._target_exclude

    @property
    def edge_name_dyad(self):
        return self.source, self.target


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


class GraphConfig:
    def __init__(self, econfig, vconfig: VertexConfig, jconfig=None):
        """

        :param econfig: edges config : direct definitions of edges
        :param vconfig: specification of vcollections
        :param jconfig: in json config edges might be defined locally,
                            so json schema should be parsed for flat edges list
        """
        self._edges: dict[tuple[str, str], Edge] = dict()

        self._exclude_fields: defaultdict[str, list] = defaultdict(list)

        self._init_edges(econfig, vconfig)
        if jconfig is not None:
            self._init_jedges(jconfig, vconfig)
        self._init_extra_edges(econfig, vconfig)
        self._init_exclude()

    def _init_edges(self, config, vconf: VertexConfig):
        if "main" in config:
            for e in config["main"]:
                edge = Edge(e, vconf)
                self._edges.update({edge.edge_name_dyad: edge})

    def _init_extra_edges(self, config, vconf: VertexConfig):
        if "extra" in config:
            for e in config["extra"]:
                edge = Edge(e, vconf, direct=False)
                self._edges.update({edge.edge_name_dyad: edge})

    def _init_exclude(self):
        for (v, w), e in self._edges.items():
            self._exclude_fields[v] += e.source_exclude
            self._exclude_fields[w] += e.target_exclude

    def _init_jedges(self, jconfig, vconf: VertexConfig):
        """
        init edges define locally in json
        :param jconfig:
        :return:
        """

        acc_edges: dict[tuple[str, str], Edge] = dict()

        acc_edges = self._parse_jedges(jconfig, acc_edges, vconf)

        self._edges.update(acc_edges)

    def _parse_jedges(
        self,
        croot,
        edge_accumulator: dict[tuple[str, str], Edge],
        vconf: VertexConfig,
    ) -> dict[tuple[str, str], Edge]:
        """
        extract edge definition and edge fields from definition dict
        :param croot:
        :param edge_accumulator:
        :return:
        """
        if isinstance(croot, dict):
            if "maps" in croot:
                for m in croot["maps"]:
                    edge_accumulator = self._parse_jedges(
                        m, edge_accumulator, vconf
                    )
            if "edges" in croot:
                for edge_dict_like in croot["edges"]:
                    edge = Edge(edge_dict_like, vconf)
                    edge_accumulator[edge.edge_name_dyad] = edge

                return edge_accumulator
        return edge_accumulator

    def graph(self, u, v) -> Edge:
        return self._edges[u, v]

    @property
    def edges(self):
        return list([k for k, v in self._edges.items() if v.type == "direct"])

    @property
    def extra_edges(self):
        return list(
            [k for k, v in self._edges.items() if v.type == "indirect"]
        )

    @property
    def all_edges(self):
        return list(self._edges)

    def exclude_fields(self, k):
        if k in self._exclude_fields:
            return self._exclude_fields[k]
        else:
            return ()
