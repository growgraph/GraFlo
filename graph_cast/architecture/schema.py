from collections import defaultdict


class CollectionIndex:
    def __init__(self, *args, **kwargs):
        self._fields = list()
        self._unique = None
        self._type = None
        if None not in args and args:
            self._fields = list(args)
        if kwargs is not None and kwargs:
            self._fields = list(kwargs["fields"])
            self._type = kwargs["type"]
            self._unique = kwargs["unique"]
        if not self._fields:
            self._fields = ["_key"]

    def check(self, fields):
        if self._fields not in set(fields + ["_key"]):
            raise ValueError(f"{self._fields} spill out of {fields}")


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
    ):
        self._name = name
        self._dbname = name if basename is None else basename
        self._fields = list(fields)
        self._index = CollectionIndex(*index)
        if extra_index is not None:
            self._extra_indices = [CollectionIndex(**item) for item in extra_index]
        self._numeric_fields = numeric_fields
        # set of filters
        self._filters = [Filter(**item) for item in filters]

    @property
    def dbname(self):
        return self._dbname

    @property
    def name(self):
        return self._name

    @property
    def index(self):
        return self._index

    @property
    def extra_indices(self):
        return iter(self._extra_indices)

    @property
    def numeric_fields(self):
        return self._numeric_fields

    @property
    def filters(self):
        return self._filters


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
        return f"{self.__class__} | field: {self.field} value: {self.value} -> foo: {self.foo}"

    __repr__ = __str__


class VertexConfig:
    _vcollections_all = []

    _vcollections = set()

    # vertex_type -> vertex_collection_name
    _vmap = {}

    # vertex_collection_name -> indices
    _index_fields_dict = {}

    # vertex_collection_name -> extra_index
    _extra_indices = {}

    # vertex_collection_name -> fields
    _vfields = {}

    # vertex_collection_name -> [numeric fields]
    _vcollection_numeric_fields_map = {}

    # list of blank collections
    _blank_collections = set()

    def __init__(self, vconfig):
        config = vconfig["collections"]

        self._init_vcollections(config)
        self._init_names(config)
        self._init_indices(config)
        self._init_extra_indexes(config)

        self._init_fields(config)
        self._init_numeric_fields(config)
        if "blanks" in vconfig:
            self._init_blank_collections(vconfig["blanks"])

    @property
    def collections(self):
        return self._vcollections

    def _init_vcollections(self, vconfig):
        self._vcollections = set(vconfig.keys())
        self._vcollections_all = {k: Vertex(name=k, **v) for k, v in vconfig.items()}

    def _init_names(self, vconfig):
        try:
            self._vmap = {
                k: v["basename"] for k, v in vconfig.items() if "basename" in v
            }
        except:
            raise KeyError(
                "vconfig does not have 'basename' for one of the vertex collections!"
            )

    def dbname(self, vertex_name):
        # old vmap
        if vertex_name in self._vcollections:
            if vertex_name in self._vmap:
                return self._vmap[vertex_name]
            else:
                return vertex_name
        else:
            raise ValueError(
                f" Accessing vertex collection names: vertex collection {vertex_name} was not defined in config"
            )

    def _init_indices(self, vconfig):
        self._index_fields_dict = {
            k: v["index"] for k, v in vconfig.items() if "index" in v
        }

    def index(self, vertex_name):
        # old index_fields_dict
        if vertex_name in self._vcollections:
            if vertex_name in self._index_fields_dict:
                return self._index_fields_dict[vertex_name]
            else:
                return ["_key"]
        else:
            raise ValueError(
                f" Accessing vertex collection indexes: vertex collection {vertex_name} was not defined in config"
            )

    def _init_extra_indexes(self, vconfig):
        self._extra_indices = {
            k: v["extra_index"] for k, v in vconfig.items() if "extra_index" in v
        }

    def extra_index_list(self, vertex_name):
        # old index_fields_dict
        if vertex_name in self._vcollections:
            if vertex_name in self._extra_indices:
                return self._extra_indices[vertex_name]
            else:
                return ()
        else:
            raise ValueError(
                f" Accessing vertex collection indexes: vertex collection {vertex_name} was not defined in config"
            )

    def _init_blank_collections(self, vconfig):
        self._blank_collections = set(vconfig)
        if set(self._blank_collections) - set(self._vcollections):
            raise ValueError(
                f" Blank collections {self.blank_collections} are not defined as vertex collections"
            )

    @property
    def blank_collections(self):
        return iter(self._blank_collections)

    def _init_fields(self, vconfig):
        self._vfields = {k: v["fields"] for k, v in vconfig.items() if "fields" in v}

    def fields(self, vertex_name):
        if vertex_name in self._vcollections:
            if vertex_name in self._vfields:
                return self._vfields[vertex_name]
            else:
                return ()
        else:
            raise ValueError(
                f" Accessing vertex collection fields: vertex collection {vertex_name} was not defined in config"
            )

    def _init_numeric_fields(self, vconfig):
        self._vcollection_numeric_fields_map = {
            k: v["numeric_fields"] for k, v in vconfig.items() if "numeric_fields" in v
        }

    def numeric_fields_list(self, vertex_name):
        if vertex_name in self._vcollections:
            if vertex_name in self._vcollection_numeric_fields_map:
                return self._vcollection_numeric_fields_map[vertex_name]
            else:
                return ()
        else:
            raise ValueError(
                f" Accessing vertex collection numeric fields: vertex collection {vertex_name} was not defined in config"
            )

    def filters(self):
        return (
            (vcol, f)
            for vcol, item in self._vcollections_all.items()
            for f in item.filters
        )


class GraphConfig:
    _edges = set()

    _extra_edges = set()

    _graphs = dict()

    _exclude_fields = defaultdict(list)

    def __init__(self, econfig, vmap, jconfig=None):
        if jconfig:
            self._init_jedges(jconfig)
        else:
            self._init_edges(econfig)
        self._init_extra_edges(econfig)
        self._check_edges_extra_edges_consistency()
        self._define_graphs(econfig, vmap)

    def _init_edges(self, config):
        # check that the edges are unique
        if "main" in config:
            self._edges = [(item["source"], item["target"]) for item in config["main"]]
            if len(set(self._edges)) < len(self._edges):
                raise ValueError(f" Potentially duplicate edges in edges definition")
            self._edges = set(self._edges)

    def _init_extra_edges(self, config):
        if "extra" in config:
            self._extra_edges = [
                (item["source"], item["target"]) for item in config["extra"]
            ]
            if len(set(self._extra_edges)) < len(self._extra_edges):
                raise ValueError(
                    f" Potentially duplicate edges in extra edges definition"
                )
            self._extra_edges = set(self._extra_edges)

    def _check_edges_extra_edges_consistency(self):
        if len(set(list(self._edges) + list(self._extra_edges))) < len(
            list(self._edges) + list(self._extra_edges)
        ):
            raise ValueError(
                f" Potentially duplicate edges between edges and extra edges definition"
            )

    def _init_jedges(self, jconfig):
        acc_edges = set()
        exclude_fields = defaultdict(list)

        self._edges, self._exclude_fields = self._parse_jedges(
            jconfig, acc_edges, exclude_fields
        )

    def _parse_jedges(self, croot, edge_accumulator, exclusion_fields):
        # TODO push mapping_fields etc to architecture
        """
        extract edge definition and edge fields from definition dict
        :param croot:
        :param edge_accumulator:
        :param exclusion_fields:
        :return:
        """
        if isinstance(croot, dict):
            if "maps" in croot:
                for m in croot["maps"]:
                    edge_acc_, exclusion_fields = self._parse_jedges(
                        m, edge_accumulator, exclusion_fields
                    )
                    edge_accumulator |= edge_acc_
            if "edges" in croot:
                edge_acc_ = set()
                for evw in croot["edges"]:
                    vname, wname = evw["source"]["name"], evw["target"]["name"]
                    edge_acc_ |= {(vname, wname)}
                    if "field" in evw["source"]:
                        exclusion_fields[vname] += [evw["source"]["field"]]
                    if "field" in evw["target"]:
                        exclusion_fields[wname] += [evw["target"]["field"]]
                return edge_acc_ | edge_accumulator, exclusion_fields
            else:
                return set(), defaultdict(list)

    def _define_graphs(self, config, vmap):

        aux = []
        if "main" in config:
            aux.extend(config["main"])
        else:
            aux.extend([{"source": u, "target": v} for u, v in self._edges])
        if "extra" in config:
            aux.extend(config["extra"])

        aux_dict = {(item["source"], item["target"]): item for item in aux}

        for (u_, v_), item in aux_dict.items():
            u, v = vmap(u_), vmap(v_)

            self._graphs[u_, v_] = {
                "source": u,
                "target": v,
                "edge_name": f"{u}_{v}_edges",
                "graph_name": f"{u}_{v}_graph",
                "type": "direct" if (u_, v_) in self._edges else "indirect",
            }
            if "weight" in item:
                self._graphs[u_, v_].update({"weight": item["weight"]})

            if (u_, v_) in self._extra_edges:
                self._graphs[u_, v_].update({"by": vmap(item["by"])})
            if "index" in item:
                self._graphs[u_, v_]["index"] = item["index"]

    def graph(self, u, v):
        try:
            return self._graphs[u, v]
        except:
            raise KeyError(f" Requested graph {u, v} not present in GraphConfig")

    def weights(self, u, v):
        if (u, v) in self._graphs and "weight" in self._graphs[u, v]:
            return self._graphs[u, v]["weight"]
        else:
            return []

    @property
    def edges(self):
        return list(self._edges)

    @property
    def extra_edges(self):
        return list(self._extra_edges)

    @property
    def all_edges(self):
        return list(self._edges) + list(self._extra_edges)

    def exclude_fields(self, k):
        if k in self._exclude_fields:
            return self._exclude_fields[k]
        else:
            return ()
