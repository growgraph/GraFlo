from collections import defaultdict
from copy import deepcopy
from itertools import permutations
from os import listdir
from os.path import isfile, join

from graph_cast.architecture.general import (
    Configurator,
    DataSourceType,
    LocalVertexCollections,
)
from graph_cast.architecture.transform import Transform


class TConfigurator(Configurator):
    def __init__(self, config):
        super().__init__(config)
        self.mode = None
        self.modes2collections = defaultdict(LocalVertexCollections)

        # table_type -> [{collection: cname, collection_maps: maps}]
        if DataSourceType.TABLE in config:
            config_table = deepcopy(config[DataSourceType.TABLE])
        else:
            raise KeyError("expected `table` section in config missing")

        self.modes2graphs = defaultdict(list)
        self.mode2files = defaultdict(list)
        self.table_config = TablesConfig(config_table, self.graph_config)
        self._init_modes2graphs(config_table, self.graph_config.direct_edges)

    def set_mode(self, mode):
        """
        TConfigurator configure several types of tables, mode tells TConfigurator
        which type to table to deal with currently
        :param mode:
        :return:
        """
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
            return []

    @property
    def current_transformations(self):
        return self.table_config.transforms(self.mode)

    def _init_modes2graphs(self, subconfig, edges):
        for item in subconfig:
            table_type = item["tabletype"]

            vcols = [iitem["type"] for iitem in item["vertex_collections"]]
            # here transform into standard form [{"collection": col_name, "map" map}]
            # from [{"collection": col_name, "maps" maps}] (where many maps are applied)
            self.modes2collections[table_type] = LocalVertexCollections(
                item["vertex_collections"]
            )
            for u, v in permutations(vcols, 2):
                if (u, v) in edges:
                    self.modes2graphs[table_type] += [(u, v)]

        self.modes2graphs = {
            k: list(set(v)) for k, v in self.modes2graphs.items()
        }

    def discover_files(self, fpath, limit_files=None):
        for keyword in self.modes2graphs:
            if keyword == "_all":
                search_pattern = ""
            else:
                search_pattern = keyword
            self.mode2files[keyword] = sorted(
                [
                    join(fpath, f)
                    for f in listdir(fpath)
                    if isfile(join(fpath, f))
                    and (search_pattern in f)
                    and ("csv" in f)
                ]
            )

        if limit_files:
            self.mode2files = {
                k: v[:limit_files] for k, v in self.mode2files.items()
            }

    def set_current_resource_name(self, tabular_resource):
        self.current_fname = tabular_resource
        for mode, vcol_resource in self.modes2collections.items():
            vcol_resource.update_mappers(filename=self.current_fname)


class TablesConfig:
    _tables: set[str] = set()

    # table_type -> [ {vertex_collection :vc, map: (table field -> collection field)} ]
    # vertex_collection -> (table field -> collection field)
    table_collection_maps: dict[str, dict[str, str]] = dict()

    # table_type -> encoding
    encodings_map: dict[str, str] = dict()

    # table_type -> vertex_collections
    _vertices: dict[str, str] = {}

    # table_type -> edge_collections
    _edges: defaultdict[str, list[tuple[str, str]]] = defaultdict(list)

    # table_type -> transforms
    _transforms: defaultdict[str, list[dict[str, str]]] = defaultdict(list)

    def __init__(self, vconfig, graph_config):
        self._init_tables(vconfig)
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

    def _init_transformations(self, subconfig):
        for item in subconfig:
            if "transforms" in item:
                for citem in item["transforms"]:
                    if "maps" in citem:
                        cmaps = deepcopy(citem["maps"])
                        for cmap in cmaps:
                            kwargs = deepcopy(citem)
                            kwargs["input"] = cmap["input"]
                            if "output" in cmap:
                                kwargs["output"] = cmap["output"]
                            self._transforms[item["tabletype"]] += [
                                Transform(**kwargs)
                            ]
                    else:
                        self._transforms[item["tabletype"]] += [
                            Transform(**citem)
                        ]

    def _init_encodings(self, subconfig):
        for item in subconfig:
            if "encoding" in item:
                self.encodings_map[item["tabletype"]] = item["encoding"]
            else:
                self.encodings_map[item["tabletype"]] = None

    def vertices(self, table_type):
        return self._vertices[table_type]

    def edges(self, table_type):
        return self._edges[table_type]

    def transforms(self, table_type):
        if table_type in self._transforms:
            return self._transforms[table_type]
        else:
            return dict()


# # TODO move atomic operation from TablesConfig
# class TableConfig:
#     logic = {}
#
#     def __init__(self):
#         pass
#
#     def vertices(self, table_type):
#         return self._vertices[table_type]
#
#     def edges(self, table_type):
#         return self._edges[table_type]
#
#     def transforms(self, table_type):
#         if table_type in self._transforms:
#             return self._transforms[table_type]
#         else:
#             return dict()
