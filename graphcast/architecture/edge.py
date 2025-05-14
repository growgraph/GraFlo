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

    aux: bool = (
        False  # aux=True edges are init in the db but not considered by graphcast
    )

    casting_type: EdgeCastingType = EdgeCastingType.PAIR_LIKE
    by: Optional[str] = None
    source_collection: Optional[str] = None
    target_collection: Optional[str] = None
    graph_name: Optional[str] = None
    collection_name: Optional[str] = None
    db_flavor: DBFlavor = DBFlavor.ARANGO

    def __post_init__(self):
        if (
            self.source_relation_field is not None
            and self.target_relation_field is not None
        ):
            raise ValueError(
                f"Both source_relation_field and target_relation_field are set for edge ({self.source}, {self.target})"
            )

    def finish_init(self, vertex_config: VertexConfig):
        """

        Args:
            vertex_config:

            discriminant is used to pin docs among a collection of docs of the same vertex type

        Returns:

        """
        if self.type == EdgeType.INDIRECT and self.by is not None:
            self.by = vertex_config.vertex_dbname(self.by)

        if self.source_discriminant is None and self.target_discriminant is None:
            self.casting_type = EdgeCastingType.PAIR_LIKE
        else:
            self.casting_type = EdgeCastingType.PRODUCT_LIKE

        if self.weights is not None:
            if self.weights.source_fields:
                vertex_config[self.source] = vertex_config[
                    self.source
                ].update_aux_fields(self.weights.source_fields)
            if self.weights.target_fields:
                vertex_config[self.target] = vertex_config[
                    self.target
                ].update_aux_fields(self.weights.target_fields)

        self.source_collection = vertex_config.vertex_dbname(self.source)
        self.target_collection = vertex_config.vertex_dbname(self.target)
        graph_name = [
            vertex_config.vertex_dbname(self.source),
            vertex_config.vertex_dbname(self.target),
        ]
        if self.relation is not None:
            graph_name += [self.relation]
        self.graph_name = "_".join(graph_name + ["graph"])
        self.collection_name = "_".join(graph_name + ["edges"])
        self.db_flavor = vertex_config.db_flavor
        self._init_indices(vertex_config)

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

        # "@" is reserved : quick hack - do not reinit the index twice
        if any("@" in f for f in index.fields):
            return index
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
    edges: list[Edge] = dataclasses.field(default_factory=list)

    def __post_init__(self):
        self._edges_map: dict[EdgeId, Edge] = {e.edge_id: e for e in self.edges}

    def finish_init(self, vc: VertexConfig):
        for k, e in self._edges_map.items():
            e.finish_init(vc)

    def _reset_edges(self):
        self.edges = list(self._edges_map.values())

    def edges_list(self, include_aux=False):
        return (e for e in self._edges_map.values() if include_aux or not e.aux)

    def edges_items(self, include_aux=False):
        return (
            (eid, e) for eid, e in self._edges_map.items() if include_aux or not e.aux
        )

    def __contains__(self, item: EdgeId | Edge):
        if isinstance(item, Edge):
            eid = item.edge_id
        else:
            eid = item

        if eid in self._edges_map:
            return True
        else:
            return False

    def update_edges(self, edge: Edge, vertex_config: VertexConfig):
        if edge.edge_id in self._edges_map:
            self._edges_map[edge.edge_id].update(edge)
        else:
            self._edges_map[edge.edge_id] = edge
        self._edges_map[edge.edge_id].finish_init(vertex_config=vertex_config)

    @property
    def vertices(self):
        return {e.source for e in self.edges} | {e.target for e in self.edges}

    # def __getitem__(self, key: EdgeId):
    #     if key in self._reset_edges():
    #         return self._edges_map[key]
    #     else:
    #         raise KeyError(f"Vertex {key} absent")
    #
    # def __setitem__(self, key: EdgeId, value: Edge):
    #     self._edges_map[key] = value
