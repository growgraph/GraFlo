"""Vertex configuration and management for graph databases.

This module provides classes and utilities for managing vertices in graph databases.
It handles vertex configuration, field management, indexing, and filtering operations.
The module supports both ArangoDB and Neo4j through the DBFlavor enum.

Key Components:
    - Vertex: Represents a vertex with its fields and indexes
    - VertexConfig: Manages collections of vertices and their configurations

Example:
    >>> vertex = Vertex(name="user", fields=["id", "name"])
    >>> config = VertexConfig(vertices=[vertex])
    >>> fields = config.fields("user", with_aux=True)
"""

import dataclasses
import logging
from typing import Optional

from graphcast.architecture.onto import Index
from graphcast.filter.onto import Expression
from graphcast.onto import BaseDataclass, DBFlavor

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class Vertex(BaseDataclass):
    """Represents a vertex in the graph database.

    A vertex is a fundamental unit in the graph that can have fields, indexes,
    and filters

    Attributes:
        name: Name of the vertex
        fields: List of field names
        fields_aux: List of auxiliary field names for weight passing
        indexes: List of indexes for the vertex
        filters: List of filter expressions
        dbname: Optional database name (defaults to vertex name)
    """

    name: str
    fields: list[str]
    fields_aux: list[str] = dataclasses.field(
        default_factory=list
    )  # temporary field necessary to pass weights to edges
    indexes: list[Index] = dataclasses.field(default_factory=list)
    filters: list[Expression] = dataclasses.field(default_factory=list)
    dbname: Optional[str] = None

    @property
    def fields_all(self):
        """Get all fields including auxiliary fields.

        Returns:
            list[str]: Combined list of regular and auxiliary fields
        """
        return self.fields + self.fields_aux

    def __post_init__(self):
        """Initialize the vertex after dataclass initialization.

        Sets the database name if not provided and updates fields based on indexes.
        """
        if self.dbname is None:
            self.dbname = self.name
        union_fields = set(self.fields)
        if not self.indexes:
            self.indexes = [Index(fields=self.fields)]
        for ei in self.indexes:
            union_fields |= set(ei.fields)
        self.fields = list(union_fields)

    def update_aux_fields(self, fields_aux: list):
        """Update auxiliary fields.

        Args:
            fields_aux: List of new auxiliary fields to add

        Returns:
            Vertex: Self for method chaining
        """
        self.fields_aux = list(set(self.fields_aux) | set(fields_aux))
        return self


@dataclasses.dataclass
class VertexConfig(BaseDataclass):
    """Configuration for managing collections of vertices.

    This class manages a collection of vertices, providing methods for accessing
    and manipulating vertex configurations.

    Attributes:
        vertices: List of vertex configurations
        blank_vertices: List of blank vertex names
        force_types: Dictionary mapping vertex names to type lists
        db_flavor: Database flavor (ARANGO or NEO4J)
    """

    vertices: list[Vertex]
    blank_vertices: list[str] = dataclasses.field(default_factory=list)
    force_types: dict[str, list] = dataclasses.field(default_factory=dict)
    db_flavor: DBFlavor = DBFlavor.ARANGO

    def __post_init__(self):
        """Initialize the vertex configuration.

        Creates internal mappings and validates blank vertices.

        Raises:
            ValueError: If blank vertices are not defined in the configuration
        """
        self._vertices_map: dict[str, Vertex] = {
            item.name: item for item in self.vertices
        }

        # TODO replace by types
        # vertex_collection_name -> [numeric fields]
        self._vcollection_numeric_fields_map = {}

        if set(self.blank_vertices) - set(self.vertex_set):
            raise ValueError(
                f" Blank collections {self.blank_vertices} are not defined"
                " as vertex collections"
            )

    @property
    def vertex_set(self):
        """Get set of vertex names.

        Returns:
            set[str]: Set of vertex names
        """
        return set(self._vertices_map.keys())

    @property
    def vertex_list(self):
        """Get list of vertex configurations.

        Returns:
            list[Vertex]: List of vertex configurations
        """
        return list(self._vertices_map.values())

    def vertex_dbname(self, vertex_name):
        """Get database name for a vertex.

        Args:
            vertex_name: Name of the vertex

        Returns:
            str: Database name for the vertex

        Raises:
            KeyError: If vertex is not found
        """
        try:
            value = self._vertices_map[vertex_name].dbname
        except KeyError as e:
            logger.error(
                "Available vertex collections :"
                f" {self._vertices_map.keys()}; vertex collection"
                f" requested : {vertex_name}"
            )
            raise e
        return value

    def index(self, vertex_name) -> Index:
        """Get primary index for a vertex.

        Args:
            vertex_name: Name of the vertex

        Returns:
            Index: Primary index for the vertex
        """
        return self._vertices_map[vertex_name].indexes[0]

    def indexes(self, vertex_name) -> list[Index]:
        """Get all indexes for a vertex.

        Args:
            vertex_name: Name of the vertex

        Returns:
            list[Index]: List of indexes for the vertex
        """
        return self._vertices_map[vertex_name].indexes

    def fields(self, vertex_name: str, with_aux=False):
        """Get fields for a vertex.

        Args:
            vertex_name: Name of the vertex
            with_aux: Whether to include auxiliary fields

        Returns:
            list[str]: List of fields
        """
        if with_aux:
            return self._vertices_map[vertex_name].fields_all
        else:
            return self._vertices_map[vertex_name].fields

    def numeric_fields_list(self, vertex_name):
        """Get list of numeric fields for a vertex.

        Args:
            vertex_name: Name of the vertex

        Returns:
            tuple: Tuple of numeric field names

        Raises:
            ValueError: If vertex is not defined in config
        """
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
        """Get filter expressions for a vertex.

        Args:
            vertex_name: Name of the vertex

        Returns:
            list[Expression]: List of filter expressions
        """
        if vertex_name in self._vertices_map:
            return self._vertices_map[vertex_name].filters
        else:
            return []

    def update_vertex(self, v: Vertex):
        """Update vertex configuration.

        Args:
            v: Vertex configuration to update
        """
        self._vertices_map[v.name] = v

    def __getitem__(self, key: str):
        """Get vertex configuration by name.

        Args:
            key: Vertex name

        Returns:
            Vertex: Vertex configuration

        Raises:
            KeyError: If vertex is not found
        """
        if key in self._vertices_map:
            return self._vertices_map[key]
        else:
            raise KeyError(f"Vertex {key} absent")

    def __setitem__(self, key: str, value: Vertex):
        """Set vertex configuration by name.

        Args:
            key: Vertex name
            value: Vertex configuration
        """
        self._vertices_map[key] = value
