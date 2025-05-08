import dataclasses
from typing import Optional

from graphcast.architecture.onto import (
    BaseDataclass,
    EdgeCastingType,
    EdgeId,
    EdgeType,
    Index,
    Weight,
)
from graphcast.architecture.vertex import VertexConfig
from graphcast.onto import DBFlavor


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
    weights: Optional[WeightConfig] = None

    non_exclusive: list[str] = dataclasses.field(default_factory=list)
    relation: Optional[str] = None

    source_discriminant: Optional[str] = None
    target_discriminant: Optional[str] = None

    source_relation_field: Optional[str] = None
    target_relation_field: Optional[str] = None

    type: EdgeType = EdgeType.DIRECT
    casting_type: EdgeCastingType = EdgeCastingType.PAIR_LIKE
    by: Optional[str] = None
    collection_name_suffix: Optional[str] = None
    source_collection: Optional[str] = None
    target_collection: Optional[str] = None
    graph_name: Optional[str] = None
    collection_name: Optional[str] = None
    db_flavor: DBFlavor = DBFlavor.ARANGO

    def __post_init__(self):
        self.source_fields: list[str]
        self.target_fields: list[str]
        if (
            self.source_relation_field is not None
            and self.target_relation_field is not None
        ):
            raise ValueError(
                f"Both source_relation_field and target_relation_field are set for edge ({self.source}, {self.target})"
            )

    def finish_init(
        self, vc: VertexConfig, same_level_vertices: list[str] | None = None
    ):
        """

        Args:
            vc:
            same_level_vertices: help to decide on how the edge will be defined:
                product : cartesian product, e.g. relation of a publication to references {id : [id_a, ...]}
                pairwise:  pairwise, e.g. when data is a list of docs : [{person: a, company: b}, ...]

            discriminant is used to pin docs among a collection of docs of the same vertex type

        Returns:

        """
        if self.type == EdgeType.INDIRECT and self.by is not None:
            self.by = vc.vertex_dbname(self.by)

        same_level_vertices = [] if same_level_vertices is None else same_level_vertices
        if (
            self.source in same_level_vertices
            and self.target in same_level_vertices
            and self.source_discriminant is None
            and self.target_discriminant is None
        ):
            self.casting_type = EdgeCastingType.PAIR_LIKE
        else:
            self.casting_type = EdgeCastingType.PRODUCT_LIKE
        self.source_collection = vc.vertex_dbname(self.source)
        self.target_collection = vc.vertex_dbname(self.target)
        graph_name = [
            vc.vertex_dbname(self.source),
            vc.vertex_dbname(self.target),
        ]
        if self.collection_name_suffix is not None:
            graph_name += [self.collection_name_suffix]
        self.graph_name = "_".join(graph_name + ["graph"])
        self.collection_name = "_".join(graph_name + ["edges"])
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

        index_fields = []

        if index.name is None:
            index_fields += index.fields
        else:
            # add index over a vertex of index.name
            if index.fields:
                fields = index.fields
            else:
                fields = vc.index(index.name).fields
            index_fields += [f"{index.name}@{x}" for x in fields]

        if not index.exclude_edge_endpoints and self.db_flavor == DBFlavor.ARANGO:
            if all([item not in index_fields for item in ["_from", "_to"]]):
                index_fields = ["_from", "_to"] + index_fields

        index.fields = index_fields
        return index

    @property
    def edge_name_dyad(self):
        return self.source, self.target

    @property
    def edge_id(self) -> EdgeId:
        return self.source, self.target, self.relation


@dataclasses.dataclass
class EdgeConfig(BaseDataclass):
    # TODO extra_edges are not used?
    edges: list[Edge] = dataclasses.field(default_factory=list)
    extra_edges: list[Edge] = dataclasses.field(default_factory=list)

    def __post_init__(self):
        self._map: dict[EdgeId, Edge] = {e.edge_id: e for e in self.edges}

    def finish_init(self, vc: VertexConfig):
        for k, e in self._map.items():
            e.finish_init(vc)

        for e in self.extra_edges:
            e.finish_init(vc)

    def _reset_edges(self):
        self.edges = list(self._map.values())

    def update_edges(self, edge: Edge):
        if edge.edge_id in self._map:
            self._map[edge.edge_id].update(edge)
        else:
            self._map[edge.edge_id] = edge

    @property
    def vertices(self):
        return {e.source for e in self.edges} | {e.target for e in self.edges}
