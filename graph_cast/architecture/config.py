from collections import defaultdict, ChainMap
from itertools import permutations, product


class Configurator:
    # table_type -> [{collection: cname, collection_maps: maps}]
    modes2collections = defaultdict(list)
    modes2graphs = defaultdict(list)

    weights_definition = {}

    mode = None

    def __init__(self, config):
        self.vertex_config = VertexConfig(config["vertex_collections"])
        self.graph_config = GraphConfig(
            config["edge_collections"], self.vertex_config.name
        )
        self.table_config = TableConfig(config["csv"])
        self._init_modes2graphs(config["csv"], self.graph_config.edges)

    def set_mode(self, mode):
        self.mode = mode

    @property
    def encoding(self):
        if self.mode in self.table_config.encodings_map:
            return self.table_config.encodings_map[self.mode]
        else:
            return None

    @property
    def current_graphs(self):
        if self.mode in self.modes2graphs:
            return self.modes2graphs[self.mode]
        else:
            return []

    @property
    def current_collections(self):
        if self.mode in self.modes2collections:
            return self.modes2collections[self.mode]
        else:
            return None

    @property
    def current_transformations(self):
        if self.mode in self.table_config.transform_maps:
            return self.table_config.transform_maps[self.mode]
        else:
            return None

    @property
    def current_weights(self):
        if self.mode in self.weights_definition:
            return self.weights_definition[self.mode]
        else:
            return None

    @property
    def current_field_maps(self):
        if self.mode in self.table_config.tables:
            return self.table_config.table_collection_maps[self.mode]
        else:
            return None

    def graph(self, u, v):
        return self.graph_config.graph(u, v)

    def _init_modes2graphs(self, subconfig, edges):

        for item in subconfig:
            table_type = item["tabletype"]

            vcols = [iitem["type"] for iitem in item["vertex_collections"]]
            # here transform into standard form [{"collection": col_name, "map" map}]
            # from [{"collection": col_name, "maps" maps}] (where many maps are applied)
            self.modes2collections[table_type] = item["vertex_collections"]
            for u, v in permutations(vcols, 2):
                if (u, v) in edges:
                    self.modes2graphs[table_type] += [(u, v)]
                # else:
                #     raise ValueError(f"{u, v} edges in {table_type} is not found definion of graphs")

        self.modes2graphs = {k: list(set(v)) for k, v in self.modes2graphs.items()}


class TableConfig:
    _tables = set()

    # table_type -> [ {vertex_collection :vc, map: (table field -> collection field)} ]
    # vertex_collection -> (table field -> collection field)
    table_collection_maps = dict()

    # table_type -> transforms
    transform_maps = dict()

    # table_type -> transforms
    encodings_map = dict()

    # table_type -> extra logic
    logic = {}

    def __init__(self, vconfig):
        self._init_tables(vconfig)
        self._init_transformations(vconfig)
        self._init_input_output_field_map(vconfig)
        self._init_transformations(vconfig)
        self._init_encodings(vconfig)

    @property
    def tables(self):
        return self._tables

    def _init_tables(self, vconfig):
        self._tables = set(
            [item["tabletype"] for item in vconfig if "tabletype" in item]
        )

    # TODO verify against VertexConfig
    # TODO not called currently - bring maps from vertex_collection definition to the current table
    # work out encapsulation
    def _init_input_output_field_map(self, subconfig):
        for item in subconfig:
            self.table_collection_maps[item["tabletype"]] = [
                {"type": vc["type"], "map": vc["map_fields"]}
                for vc in item["vertex_collections"]
                if "map_fields" in vc
            ]

    def _init_transformations(self, subconfig):
        for item in subconfig:
            if "transforms" in item:
                self.transform_maps[item["tabletype"]] = item["transforms"]

    def _init_encodings(self, subconfig):
        for item in subconfig:
            if "encoding" in item:
                self.encodings_map[item["tabletype"]] = item["encoding"]
            else:
                self.encodings_map[item["tabletype"]] = None

    def parse_logic(self, subconfig):
        for item in subconfig:
            if "logic" in item:
                self.logic[item["tabletype"]] = item["logic"]
            else:
                self.logic[item["tabletype"]] = None


class VertexConfig:
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

    def _init_names(self, vconfig):
        try:
            self._vmap = {
                k: v["basename"] for k, v in vconfig.items() if "basename" in v
            }
        except:
            raise KeyError(
                "vconfig does not have 'basename' for one of the vertex collections!"
            )

    def name(self, vertex_name):
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


class GraphConfig:
    _edges = set()

    _extra_edges = set()

    _graphs = dict()

    def __init__(self, econfig, vmap):
        self._init_edges(econfig)
        self._define_graphs(econfig, vmap)

    def _init_edges(self, config):
        # check that the edges are unique
        if "main" in config:
            self._edges = [(item["source"], item["target"]) for item in config["main"]]
            if len(set(self._edges)) < len(self._edges):
                raise ValueError(f" Potentially duplicate edges in edges definition")
            self._edges = set(self._edges)
        if "extra" in config:
            self._extra_edges = [
                (item["source"], item["target"]) for item in config["extra"]
            ]
            if len(set(self._extra_edges)) < len(self._extra_edges):
                raise ValueError(
                    f" Potentially duplicate edges in extra edges definition"
                )
            self._extra_edges = set(self._extra_edges)
        if len(set(list(self._edges) + list(self._extra_edges))) < len(
            list(self._edges) + list(self._extra_edges)
        ):
            raise ValueError(
                f" Potentially duplicate edges between edges and extra edges definition"
            )

    def _define_graphs(self, config, vmap):
        aux = []
        if "main" in config:
            aux.extend(config["main"])
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
            if (u_, v_) in self._extra_edges:
                self._graphs[u_, v_].update(
                    {"by": vmap(item["by"]), "edge_weight": item["edge_weight"]}
                )
            if "index" in item:
                self._graphs[u_, v_]["index"] = item["index"]

    def graph(self, u, v):
        try:
            return self._graphs[u, v]
        except:
            raise KeyError(f" Requested graph {u, v} not present in GraphConfig")

    @property
    def edges(self):
        return list(self._edges)

    @property
    def extra_edges(self):
        return list(self._extra_edges)

    @property
    def all_edges(self):
        return list(self._edges) + list(self._extra_edges)
