import logging
from collections import defaultdict
from copy import deepcopy
from os import listdir
from os.path import isfile, join
from typing import Iterator

import networkx as nx

from graph_cast.architecture.general import (
    Configurator,
    DataSourceType,
    VertexConfig,
)
from graph_cast.architecture.schema import EncodingType
from graph_cast.architecture.transform import Transform

logger = logging.getLogger(__name__)


class TConfigurator(Configurator):
    def __init__(self, config):
        super().__init__(config)
        self.active_table_type = None

        if DataSourceType.TABLE in config:
            config_table = deepcopy(config[DataSourceType.TABLE])
        else:
            raise KeyError("expected `table` section in config missing")

        self.mode2files = defaultdict(list)
        self.table_config: dict[str, TableConfig] = dict()
        self._init_table_configs(config_table)

    def set_mode(self, mode):
        """
        TConfigurator configure several types of tables, mode tells TConfigurator
        which type to table to deal with currently
        :param mode:
        :return:
        """
        self.active_table_type = mode

    def _init_table_configs(self, config_tables):
        for item in config_tables:
            tc = TableConfig(item, self.vertex_config)
            self.table_config[tc.table_type] = tc

        self._all_vertices = set()
        for vs in self.table_config.values():
            self._all_vertices |= set(vs.vertices)

    @property
    def encoding(self):
        if self.active_table_type in self.table_config:
            return self.table_config[self.active_table_type].encoding
        else:
            return None

    @property
    def current_edges(self):
        return self.graph_config.edge_projection(
            self.vertices(self.active_table_type)
        )

    @property
    def current_transformations(self) -> Iterator[Transform]:
        return self.table_config[self.active_table_type].transforms()

    def discover_files(self, fpath, limit_files=None):
        for keyword in self.tables:
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

        if limit_files is not None:
            self.mode2files = {
                k: v[:limit_files] for k, v in self.mode2files.items()
            }

    def exclude_fields(self, k):
        return self.graph_config.exclude_fields(k)

    @property
    def tables(self):
        return list(self.table_config.keys())

    def vertices(self, table_name: str | None = None):
        if table_name is not None and table_name:
            return self.table_config[table_name].vertices
        else:
            return self._all_vertices


class TableConfig:
    def __init__(
        self,
        config_table,
        vertex_config: VertexConfig,
    ):
        self.encoding: EncodingType = config_table.get(
            "encoding", EncodingType.UTF_8
        )
        self.table_type = config_table.get("tabletype", None)

        if self.table_type is None:
            raise ValueError(f"tabletype absent in {config_table}")

        # table_type -> transforms
        self._transforms: dict[int, Transform] = {}

        # bipartite graph from vertices to transformations
        self._vertex_tau = nx.DiGraph()

        self._init_transformations(config_table, vertex_config)

    def _init_transformations(self, subconfig, vertex_config: VertexConfig):
        transforms = subconfig.get("transforms", [])
        for t in transforms:
            tau = Transform(**t)
            self._transforms[id(tau)] = tau
            related_collections = [
                c
                for c in vertex_config.collections
                if set(vertex_config.fields(c)) & set(tau.output)
            ]
            if len(related_collections) > 1:
                if tau.image is not None:
                    related_collections = [tau.image]
                else:
                    logger.warning(
                        f"Multiple collections {related_collections} are"
                        f" related to transformation {tau}, consider revising"
                        " your schema"
                    )
            self._vertex_tau.add_edges_from(
                [(c, id(tau)) for c in related_collections]
            )

    @property
    def vertices(self) -> set[str]:
        return set(v for v, _ in self._vertex_tau.edges)

    def transforms(self, vertex: str | None = None) -> Iterator[Transform]:
        if vertex is not None:
            neighbours = self._vertex_tau.neighbors(vertex)
        else:
            neighbours = self._transforms.keys()
        return (self._transforms[k] for k in neighbours)

    def fields(self, vertex: str | None = None) -> set[str]:
        field_sets: Iterator[set[str]]
        if vertex is None:
            field_sets = (self.fields(v) for v in self.vertices)
        else:
            neighbours = self._vertex_tau.neighbors(vertex)
            field_sets = (set(self._transforms[k].output) for k in neighbours)
        fields: set[str] = set().union(*field_sets)
        return fields
