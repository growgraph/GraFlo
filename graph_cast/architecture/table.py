import logging
from collections import defaultdict
from copy import deepcopy
from os import listdir
from os.path import isfile, join
from typing import Iterator

import networkx as nx

from graph_cast.architecture import DataSourceType
from graph_cast.architecture.general import Configurator
from graph_cast.architecture.graph import GraphConfig
from graph_cast.architecture.onto import EncodingType
from graph_cast.architecture.transform import Transform
from graph_cast.architecture.vertex import VertexConfig

logger = logging.getLogger(__name__)


class TableConfig:
    RESERVED_TAU_WEIGHTS = "_$row"

    def __init__(
        self,
        config_table,
        vertex_config: VertexConfig,
        graph_config: GraphConfig | None = None,
    ):
        self.encoding: EncodingType = config_table.get(
            "encoding", EncodingType.UTF_8
        )
        self.table_type = config_table.get("tabletype", None)

        if self.table_type is None:
            raise ValueError(f"tabletype absent in {config_table}")

        # table_type -> transforms
        self._transforms: dict[int, Transform] = {}

        # bipartite graph from vertices ( +TableConfig.RESERVED_TAU_WEIGHTS) to transformations
        self._vertex_tau = nx.DiGraph()

        self._vertices: set = set()

        self._init_transformations(config_table, vertex_config, graph_config)

    def _init_transformations(
        self,
        subconfig,
        vertex_config: VertexConfig,
        graph_config: GraphConfig | None,
    ):
        transforms = subconfig.get("transforms", [])
        for t in transforms:
            tau = Transform(**t)
            self._transforms[id(tau)] = tau
            related_vertices = [
                c
                for c in vertex_config.vertex_set
                if set(vertex_config.fields(c)) & set(tau.output)
            ]
            self._vertices |= set(related_vertices)
            if not related_vertices:
                if graph_config is not None and (
                    graph_config.weight_raw_fields() & set(tau.output)
                ):
                    related_vertices += [TableConfig.RESERVED_TAU_WEIGHTS]
            if len(related_vertices) > 1:
                if (
                    tau.image is not None
                    and tau.image in vertex_config.vertex_set
                ):
                    related_vertices = [tau.image]
                else:
                    logger.warning(
                        f"Multiple collections {related_vertices} are"
                        f" related to transformation {tau}, consider revising"
                        " your schema"
                    )
            self._vertex_tau.add_edges_from(
                [(c, id(tau)) for c in related_vertices]
            )

    def add_passthrough_transformations(
        self, keys: list[str], vertex_config: VertexConfig
    ):
        pre_vertex_fields_map = {
            vertex: set(keys) & set(vertex_config.fields(vertex))
            for vertex in vertex_config.vertex_set
        }
        for vertex, fs in pre_vertex_fields_map.items():
            tau_fields = self.fields(vertex)
            fields_passthrough = set(fs) - tau_fields
            if fields_passthrough:
                tau = Transform(
                    map=dict(zip(fields_passthrough, fields_passthrough)),
                    image=vertex,
                )
                self._transforms[id(tau)] = tau
                self._vertex_tau.add_edges_from([(vertex, id(tau))])

    def add_weight_transformations(
        self, keys: list[str], graph_config: GraphConfig
    ):
        pass

    @property
    def vertices(self) -> set[str]:
        return self._vertices

    def transforms(self, vertex: str | None = None) -> Iterator[Transform]:
        if vertex is not None:
            if vertex in self._vertex_tau.nodes:
                neighbours = self._vertex_tau.neighbors(vertex)
            else:
                return iter(())
        else:
            neighbours = self._transforms.keys()
        return (self._transforms[k] for k in neighbours)

    def fields(self, vertex: str | None = None) -> set[str]:
        field_sets: Iterator[set[str]]
        if vertex is None:
            field_sets = (self.fields(v) for v in self.vertices)
        elif vertex in self._vertex_tau.nodes:
            neighbours = self._vertex_tau.neighbors(vertex)
            field_sets = (set(self._transforms[k].output) for k in neighbours)
        else:
            return set()
        fields: set[str] = set().union(*field_sets)
        return fields


class TConfigurator(Configurator):
    def __init__(self, config):
        super().__init__(config)
        self.active_table_type = None

        if DataSourceType.TABLE in config:
            config_table = deepcopy(config[DataSourceType.TABLE])
        else:
            raise KeyError("expected `csv` section in config missing")

        self.mode2files = defaultdict(list)
        self.table_config: dict[str, TableConfig] = dict()
        self._init_table_configs(config_table)

    def set_mode(self, mode):
        """
        TConfigurator configure several types of tables, mode tells TConfigurator
        which type to csv to deal with currently
        :param mode:
        :return:
        """
        self.active_table_type = mode

    def transform_config(self) -> TableConfig:
        return self.table_config[self.active_table_type]

    def _init_table_configs(self, config_tables):
        for item in config_tables:
            tc = TableConfig(
                item,
                vertex_config=self.vertex_config,
                graph_config=self.graph_config,
            )
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
    def current_transform_config(self) -> TableConfig:
        return self.table_config[self.active_table_type]

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
