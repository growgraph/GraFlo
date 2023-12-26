from __future__ import annotations

import dataclasses

from graph_cast.architecture.onto import (
    BaseDataclass,
    EdgeMapping,
    EdgeType,
    Index,
    IndexType,
    Weight,
)
from graph_cast.architecture.vertex import VertexConfig
from graph_cast.onto import DBFlavor


@dataclasses.dataclass
class WeightConfig(BaseDataclass):
    source_fields: list[str] = dataclasses.field(default_factory=list)
    target_fields: list[str] = dataclasses.field(default_factory=list)
    vertices: list[Weight] = dataclasses.field(default_factory=list)
    direct: list[str] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class Edge(BaseDataclass):
    source: str
    target: str
    indexes: list[Index] = dataclasses.field(default_factory=list)
    weights: WeightConfig = None  # type: ignore

    non_exclusive: list[str] = dataclasses.field(default_factory=list)
    relation: str | None = None

    source_discriminant: str | None = None
    target_discriminant: str | None = None
    type: EdgeType = EdgeType.DIRECT
    by: str | None = None
    collection_name_suffix: str | None = None
    source_collection: str | None = None
    target_collection: str | None = None
    graph_name: str | None = None
    db_flavor: DBFlavor = DBFlavor.ARANGO

    def __post_init__(self):
        self.source_fields: list[str]
        self.target_fields: list[str]

    def finish_init(self, vc: VertexConfig):
        if self.type == EdgeType.INDIRECT and self.by is not None:
            self.by = vc.vertex_dbname(self.by)
        self.source_collection = vc.vertex_dbname(self.source)
        self.target_collection = vc.vertex_dbname(self.target)
        graph_name = [
            vc.vertex_dbname(self.source),
            vc.vertex_dbname(self.target),
        ]
        if self.relation is not None:
            graph_name += self.relation
        if self.collection_name_suffix is not None:
            graph_name += self.collection_name_suffix
        self.graph_name = "_".join(graph_name + ["graph"])
        self.db_flavor = vc.db_flavor
        self._init_indices(vc)

    def _init_indices(self, vc: VertexConfig):
        """
        index should be consistent with weight
        :param vc:
        :return:
        """
        self.indexes = [self._init_index(index, vc) for index in self.indexes]

    def _init_index(self, index: Index, vc: VertexConfig) -> Index:
        """
        default behavior for edge indices : add ["_from", "_to"] for uniqueness
        :param index:
        :param vc:
        :return:
        """
        if index.name is not None:
            index_fields = [
                f"{index.name}@{x}" for x in vc.index(index.name).fields
            ]
            if (
                not index.exclude_edge_end_vertices
                and self.db_flavor == DBFlavor.ARANGO
            ):
                index_fields = ["_from", "_to"] + index_fields
            index.fields = index_fields
        return index

    @property
    def source_exclude(self):
        # TODO refactor out
        return []

    @property
    def target_exclude(self):
        # TODO refactor out
        return []

    @property
    def edge_name_dyad(self):
        return self.source, self.target

    @property
    def edge_id(self) -> tuple[str, str, str | None]:
        return self.source, self.target, self.relation

    def __iadd__(self, other: Edge):
        if self.edge_name_dyad == other.edge_name_dyad:
            self.indexes += other.indexes
            # TODO revise
            # self.weights += other.weights
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
class EdgeConfig(BaseDataclass):
    edges: list[Edge] = dataclasses.field(default_factory=list)
    extra_edges: list[Edge] = dataclasses.field(default_factory=list)

    def finish_init(self, vc: VertexConfig):
        for e in self.edges:
            e.finish_init(vc)
        for e in self.extra_edges:
            e.finish_init(vc)

    @property
    def vertices(self):
        return {e.source for e in self.edges} | {e.target for e in self.edges}
