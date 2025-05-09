import dataclasses
import logging
from collections import defaultdict

from graphcast.architecture.onto import Index
from graphcast.architecture.transform import Transform
from graphcast.filter.onto import Expression
from graphcast.onto import BaseDataclass, DBFlavor

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class Vertex(BaseDataclass):
    name: str
    fields: list[str]
    indexes: list[Index] = dataclasses.field(default_factory=list)
    filters: list[Expression] = dataclasses.field(default_factory=list)
    transforms: list[Transform] = dataclasses.field(default_factory=list)
    dbname: str | None = None

    def __post_init__(self):
        if self.dbname is None:
            self.dbname = self.name
        union_fields = set(self.fields)
        for ei in self.indexes:
            union_fields |= set(ei.fields)
        self.fields = list(union_fields)


@dataclasses.dataclass
class VertexConfig(BaseDataclass):
    vertices: list[Vertex]
    blank_vertices: list[str] = dataclasses.field(default_factory=list)
    force_types: dict[str, list] = dataclasses.field(default_factory=dict)
    db_flavor: DBFlavor = DBFlavor.ARANGO

    def __post_init__(self):
        self._vertices_dict: dict[str, Vertex] = {
            item.name: item for item in self.vertices
        }
        self.vertex_set: set[str] = set(self._vertices_dict.keys())

        # TODO replace by types
        # vertex_collection_name -> [numeric fields]
        self._vcollection_numeric_fields_map = {}

        self.discriminant_chart: defaultdict[str, bool] = defaultdict(lambda: False)

        if set(self.blank_vertices) - set(self.vertex_set):
            raise ValueError(
                f" Blank collections {self.blank_vertices} are not defined"
                " as vertex collections"
            )

    def vertex_dbname(self, vertex_name):
        try:
            value = self._vertices_dict[vertex_name].dbname
        except KeyError as e:
            logger.error(
                "Available vertex collections :"
                f" {self._vertices_dict.keys()}; vertex collection"
                f" requested : {vertex_name}"
            )
            raise e
        return value

    def index(self, vertex_name) -> Index:
        return self._vertices_dict[vertex_name].indexes[0]

    def indexes(self, vertex_name) -> list[Index]:
        return self._vertices_dict[vertex_name].indexes

    def fields(self, vertex_name: str):
        return self._vertices_dict[vertex_name].fields

    def numeric_fields_list(self, vertex_name):
        if vertex_name in self.vertex_set:
            if vertex_name in self._vcollection_numeric_fields_map:
                return self._vcollection_numeric_fields_map[vertex_name]
            else:
                return ()
        else:
            raise ValueError(
                " Accessing vertex collection numeric fields: vertex"
                f" collection {vertex_name} was not defined in config"
            )

    def filters(self, vertex_name) -> list[Expression]:
        if vertex_name in self._vertices_dict:
            return self._vertices_dict[vertex_name].filters
        else:
            return []
