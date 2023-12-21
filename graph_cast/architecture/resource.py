from __future__ import annotations

import dataclasses
import logging
from collections import defaultdict
from copy import deepcopy
from typing import Iterator

import networkx as nx

from graph_cast.architecture import DataSourceType
from graph_cast.architecture.edge import Edge, EdgeConfig
from graph_cast.architecture.mapper import MapperNode
from graph_cast.architecture.onto import EncodingType, GraphEntity
from graph_cast.architecture.transform import Transform
from graph_cast.architecture.vertex import VertexConfig
from graph_cast.onto import BaseDataclass

logger = logging.getLogger(__name__)


DISCRIMINANT_KEY = "__discriminant_key"


@dataclasses.dataclass
class Resource(BaseDataclass):
    name: str | None = None
    resource_type: DataSourceType = DataSourceType.TABLE
    encoding: EncodingType = EncodingType.UTF_8


@dataclasses.dataclass
class RowResource(Resource):
    transforms: list[Transform] = dataclasses.field(default_factory=list)

    def __post_init__(self):
        self._vertices: set = set()
        self._vertex_tau = nx.DiGraph()
        self._transforms: dict[int, Transform] = {}

        self._vertex_tau_current: nx.DiGraph = nx.DiGraph()
        self._transforms_current: dict[int, Transform] = {}

    def finish_init(
        self,
        vertex_config: VertexConfig,
        edge_config: None | EdgeConfig = None,
    ):
        for tau in self.transforms:
            self._transforms[id(tau)] = tau
            related_vertices = [
                c
                for c in vertex_config.vertex_set
                if set(vertex_config.fields(c)) & set(tau.output)
            ]
            self._vertices |= set(related_vertices)
            if edge_config is not None:
                related_edges = [
                    e.edge_id
                    for e in edge_config.edges
                    if e.weights is not None
                    and (set(e.weights.direct) & set(tau.output))
                ]
            else:
                related_edges = []

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
                [(c, id(tau)) for c in related_vertices + related_edges]
            )

    def add_trivial_transformations(
        self, vertex_config: VertexConfig, header_keys: list[str]
    ):
        self._transforms_current = deepcopy(self._transforms)
        self._vertex_tau_current = self._vertex_tau.copy()

        pre_vertex_fields_map = {
            vertex: set(header_keys) & set(vertex_config.fields(vertex))
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
                self._transforms_current[id(tau)] = tau
                self._vertex_tau_current.add_edges_from([(vertex, id(tau))])

    def fetch_transforms(self, ge: GraphEntity) -> Iterator[Transform]:
        if ge in self._vertex_tau_current.nodes:
            neighbours = self._vertex_tau_current.neighbors(ge)
        else:
            return iter(())
        return (self._transforms_current[k] for k in neighbours)

    def fields(self, vertex: str | None = None) -> set[str]:
        field_sets: Iterator[set[str]]
        if vertex is None:
            field_sets = (self.fields(v) for v in self._vertices)
        elif vertex in self._vertex_tau.nodes:
            neighbours = self._vertex_tau.neighbors(vertex)
            field_sets = (set(self._transforms[k].output) for k in neighbours)
        else:
            return set()
        fields: set[str] = set().union(*field_sets)
        return fields


@dataclasses.dataclass(kw_only=True)
class TreeResource(Resource):
    root: MapperNode
    merge_collection: list[str] = dataclasses.field(default_factory=list)
    extra_weights: list[Edge] = dataclasses.field(default_factory=list)

    def __post_init__(self):
        self.resource_type = DataSourceType.JSON

    def finish_init(self, vc: VertexConfig):
        for e in self.extra_weights:
            e.finish_init(vc)

    def apply(
        self,
        doc,
        vertex_config: VertexConfig,
        acc: defaultdict[GraphEntity, list],
        discriminant_key=DISCRIMINANT_KEY,
    ):
        self.root.apply(
            doc,
            vertex_config,
            acc,
            discriminant_key,
        )


@dataclasses.dataclass
class ResourceHolder(BaseDataclass):
    row_likes: list[RowResource] = dataclasses.field(default_factory=list)
    tree_likes: list[TreeResource] = dataclasses.field(default_factory=list)

    def finish_init(self, vc: VertexConfig, ec: EdgeConfig):
        for r in self.tree_likes:
            r.finish_init(vc)
        for r in self.row_likes:
            r.finish_init(vertex_config=vc, edge_config=ec)
