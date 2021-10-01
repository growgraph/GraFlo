from collections import defaultdict
from itertools import permutations
from graph_cast.architecture.schema import VertexConfig, GraphConfig


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

        self.table_config = TableConfig(config["csv"], self.graph_config)
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
        return self.table_config.transforms(self.mode)

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

        self.modes2graphs = {k: list(set(v)) for k, v in self.modes2graphs.items()}


class TableConfig:
    _tables = set()

    # table_type -> [ {vertex_collection :vc, map: (table field -> collection field)} ]
    # vertex_collection -> (table field -> collection field)
    table_collection_maps = dict()

    # table_type -> transforms
    encodings_map = dict()

    # table_type -> extra logic
    logic = {}

    # table_type -> vertex_collections
    _vertices = {}

    # table_type -> edge_collections
    _edges = {}

    # table_type -> transforms
    _transforms = dict()

    def __init__(self, vconfig, graph_config):
        self._init_tables(vconfig)
        self._init_transformations(vconfig)
        self._init_input_output_field_map(vconfig)
        self._init_transformations(vconfig)
        self._init_encodings(vconfig)
        self._init_vertices(vconfig)
        self._init_edges(graph_config)

    @property
    def tables(self):
        return self._tables

    def _init_tables(self, vconfig):
        self._tables = set(
            [item["tabletype"] for item in vconfig if "tabletype" in item]
        )

    def _init_vertices(self, vconfig):
        self._vertices = {
            item["tabletype"]: [
                subitem["type"]
                for subitem in item["vertex_collections"]
                if "type" in subitem
            ]
            for item in vconfig
            if "tabletype" in item
        }
        # TODO run check : wrt to vertex_config

    def _init_edges(self, graph_config):
        for table, vertices in self._vertices.items():
            self._edges[table] = [
                (u, v)
                for u, v in graph_config.all_edges
                if u in vertices and v in vertices
            ]

        # run check : wrt to vertex_config

    def _init_input_output_field_map(self, subconfig):
        # TODO verify against VertexConfig
        # TODO not called currently - bring maps from vertex_collection definition to the current table
        # work out encapsulation
        for item in subconfig:
            self.table_collection_maps[item["tabletype"]] = [
                {"type": vc["type"], "map": vc["map_fields"]}
                for vc in item["vertex_collections"]
                if "map_fields" in vc
            ]

    def _init_transformations(self, subconfig):
        for item in subconfig:
            if "transforms" in item:
                self._transforms[item["tabletype"]] = item["transforms"]

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

    def vertices(self, table_type):
        return self._vertices[table_type]

    def edges(self, table_type):
        return self._edges[table_type]

    def transforms(self, table_type):
        if table_type in self._transforms:
            return self._transforms[table_type]
        else:
            return dict()


