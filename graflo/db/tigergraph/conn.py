"""TigerGraph connection implementation for graph database operations.

This module implements the Connection interface for TigerGraph, providing
specific functionality for graph operations in TigerGraph. It handles:
- Vertex and edge management
- GSQL query execution
- Schema management
- Batch operations
- Graph traversal and analytics

Key Features:
    - Vertex and edge type management
    - GSQL query execution
    - Schema definition and management
    - Batch vertex and edge operations
    - Graph analytics and traversal

Example:
    >>> conn = TigerGraphConnection(config)
    >>> conn.init_db(schema, clean_start=True)
    >>> conn.upsert_docs_batch(docs, "User", match_keys=["email"])
"""

import json
import logging
from typing import Optional

from pyTigerGraph import TigerGraphConnection as PyTigerGraphConnection

from graflo.architecture.edge import Edge
from graflo.architecture.onto import Index
from graflo.architecture.schema import Schema
from graflo.architecture.vertex import VertexConfig
from graflo.db.connection import Connection
from graflo.filter.onto import Expression
from graflo.onto import AggregationType, DBFlavor
from graflo.util.transform import pick_unique_dict

logger = logging.getLogger(__name__)


class TigerGraphConnection(Connection):
    """TigerGraph-specific implementation of the Connection interface.

    This class provides TigerGraph-specific implementations for all database
    operations, including vertex management, edge operations, and GSQL query
    execution. It uses the pyTigerGraph Python driver for all operations.

    Attributes:
        conn: TigerGraph database connection instance
        flavor: Database flavor identifier (TIGERGRAPH)
    """

    flavor = DBFlavor.TIGERGRAPH

    def __init__(self, config):
        """Initialize TigerGraph connection.

        Args:
            config: TigerGraph connection configuration containing host, credentials,
                and graph name. Expected attributes: host, graphname, username, password,
                and optionally secret, certPath
        """
        super().__init__()
        self.conn = PyTigerGraphConnection(
            host=config.host,
            graphname=config.graphname,
            username=config.username,
            password=config.password,
            certPath=getattr(config, 'certPath', None),
        )
        
        # Get authentication token if secret is provided
        if hasattr(config, 'secret') and config.secret:
            self.conn.getToken(config.secret)

    def create_database(self, name: str):
        """Create a new TigerGraph database.

        Note: TigerGraph doesn't support creating databases via API.
        This is a no-op operation.

        Args:
            name: Name of the database to create
        """
        logger.info(f"TigerGraph doesn't support creating databases via API. Graph '{name}' should be created manually.")

    def delete_database(self, name: str):
        """Delete a TigerGraph database.

        Note: TigerGraph doesn't support deleting databases via API.
        This operation clears all data instead.

        Args:
            name: Name of the database to delete
        """
        try:
            # Clear all vertices and edges
            self.execute("DELETE VERTEX *")
        except Exception as e:
            logger.error(f"Could not clear database: {e}")

    def execute(self, query, **kwargs):
        """Execute a GSQL query.

        Args:
            query: GSQL query string to execute
            **kwargs: Additional query parameters

        Returns:
            Result: TigerGraph query result
        """
        try:
            result = self.conn.runInstalledQuery(query, **kwargs)
            return result
        except Exception as e:
            logger.error(f"Error executing GSQL query: {e}")
            raise

    def close(self):
        """Close the TigerGraph connection."""
        # TigerGraph connection doesn't need explicit closing
        pass

    def init_db(self, schema: Schema, clean_start):
        """Initialize TigerGraph with the given schema.

        Args:
            schema: Schema containing graph structure definitions
            clean_start: If True, delete all existing data before initialization
        """
        if clean_start:
            self.delete_database("")
        self.define_collections(schema)
        self.define_indexes(schema)

    def define_collections(self, schema: Schema):
        """Define collections based on schema.

        Note: This is a no-op in TigerGraph as collections are implicit.

        Args:
            schema: Schema containing collection definitions
        """
        pass

    def define_vertex_collections(self, schema: Schema):
        """Define vertex collections based on schema.

        Note: This is a no-op in TigerGraph as vertex collections are implicit.

        Args:
            schema: Schema containing vertex definitions
        """
        pass

    def define_edge_collections(self, edges: list[Edge]):
        """Define edge collections based on schema.

        Note: This is a no-op in TigerGraph as edge collections are implicit.

        Args:
            edges: List of edge configurations
        """
        pass

    def define_vertex_indices(self, vertex_config: VertexConfig):
        """Define indices for vertex types.

        Creates indices for each vertex type based on the configuration.

        Args:
            vertex_config: Vertex configuration containing index definitions
        """
        for c in vertex_config.vertex_set:
            for index_obj in vertex_config.indexes(c):
                self._add_index(c, index_obj)

    def define_edge_indices(self, edges: list[Edge]):
        """Define indices for edge types.

        Creates indices for each edge type based on the configuration.

        Args:
            edges: List of edge configurations containing index definitions
        """
        for edge in edges:
            for index_obj in edge.indexes:
                if edge.relation is not None:
                    self._add_index(edge.relation, index_obj, is_vertex_index=False)

    def _add_index(self, obj_name, index: Index, is_vertex_index=True):
        """Add an index to a vertex or edge type.

        Args:
            obj_name: Vertex or edge type name
            index: Index configuration to create
            is_vertex_index: If True, create index on vertices, otherwise on edges
        """
        # TigerGraph automatically creates indices on primary keys
        # Additional indices can be created via GSQL
        fields_str = ", ".join(index.fields)
        index_name = f"{obj_name}_{'_'.join(index.fields)}"
        
        if is_vertex_index:
            query = f"CREATE INDEX {index_name} ON {obj_name} ({fields_str})"
        else:
            query = f"CREATE INDEX {index_name} ON {obj_name} ({fields_str})"
        
        try:
            self.execute(query)
        except Exception as e:
            logger.warning(f"Could not create index {index_name}: {e}")

    def delete_collections(self, cnames=(), gnames=(), delete_all=False):
        """Delete vertices and edges from the database.

        Args:
            cnames: Vertex/edge type names to delete
            gnames: Unused in TigerGraph
            delete_all: If True, delete all vertices and edges
        """
        if cnames:
            for c in cnames:
                query = f"DELETE VERTEX {c}"
                self.execute(query)
        elif delete_all:
            query = "DELETE VERTEX *"
            self.execute(query)

    def upsert_docs_batch(self, docs, class_name, match_keys, **kwargs):
        """Upsert a batch of vertices using GSQL.

        Performs an upsert operation on a batch of vertices, using the specified
        match keys to determine whether to update existing vertices or create new ones.

        Args:
            docs: List of vertex documents to upsert
            class_name: Vertex type to upsert into
            match_keys: Keys to match for upsert operation
            **kwargs: Additional options:
                - dry: If True, don't execute the query
        """
        dry = kwargs.pop("dry", False)
        
        if dry:
            return

        # TigerGraph uses UPSERT for vertex operations
        for doc in docs:
            # Create UPSERT query for each document
            match_conditions = " AND ".join([f"{k} == {json.dumps(doc.get(k))}" for k in match_keys])
            query = f"""
                UPSERT VERTEX {class_name} SET 
                {', '.join([f"{k} = {json.dumps(doc.get(k))}" for k in doc.keys()])}
                WHERE {match_conditions}
            """
            try:
                self.execute(query)
            except Exception as e:
                logger.error(f"Error upserting vertex: {e}")

    def insert_edges_batch(
        self,
        docs_edges,
        source_class,
        target_class,
        relation_name,
        collection_name=None,
        match_keys_source=("_key",),
        match_keys_target=("_key",),
        filter_uniques=True,
        uniq_weight_fields=None,
        uniq_weight_collections=None,
        upsert_option=False,
        head=None,
        **kwargs,
    ):
        """Insert a batch of edges using GSQL.

        Creates edges between source and target vertices.

        Args:
            docs_edges: List of edge documents in format [{_source_aux: source_doc, _target_aux: target_doc}]
            source_class: Source vertex type
            target_class: Target vertex type
            relation_name: Edge type name
            collection_name: Unused in TigerGraph
            match_keys_source: Keys to match source vertices
            match_keys_target: Keys to match target vertices
            filter_uniques: If True, filter duplicate edges
            uniq_weight_fields: Unused in TigerGraph
            uniq_weight_collections: Unused in TigerGraph
            upsert_option: If True, use upsert instead of insert
            head: Optional limit on number of edges to insert
            **kwargs: Additional options:
                - dry: If True, don't execute the query
        """
        dry = kwargs.pop("dry", False)
        
        if dry:
            return

        if isinstance(docs_edges, list):
            if head is not None:
                docs_edges = docs_edges[:head]
            if filter_uniques:
                docs_edges = pick_unique_dict(docs_edges)

        for edge_doc in docs_edges:
            source_doc = edge_doc.get("_source_aux", {})
            target_doc = edge_doc.get("_target_aux", {})
            edge_props = edge_doc.get("_edge_props", {})
            
            # Create edge insertion query
            source_match = " AND ".join([f"{k} == {json.dumps(source_doc.get(k))}" for k in match_keys_source])
            target_match = " AND ".join([f"{k} == {json.dumps(target_doc.get(k))}" for k in match_keys_target])
            
            edge_props_str = ", ".join([f"{k} = {json.dumps(v)}" for k, v in edge_props.items()])
            
            query = f"""
                INSERT INTO {relation_name} 
                (FROM {source_class} WHERE {source_match}, 
                 TO {target_class} WHERE {target_match})
                VALUES ({edge_props_str})
            """
            
            try:
                self.execute(query)
            except Exception as e:
                logger.error(f"Error inserting edge: {e}")

    def insert_return_batch(self, docs, class_name):
        """Insert vertices and return their properties.

        Note: Not implemented in TigerGraph.

        Args:
            docs: Documents to insert
            class_name: Vertex type to insert into

        Raises:
            NotImplementedError: This method is not implemented for TigerGraph
        """
        raise NotImplementedError("insert_return_batch not implemented for TigerGraph")

    def fetch_docs(
        self,
        class_name,
        filters: list | dict | None = None,
        limit: int | None = None,
        return_keys: list | None = None,
        unset_keys: list | None = None,
    ):
        """Fetch vertices from a type.

        Args:
            class_name: Vertex type to fetch from
            filters: Query filters
            limit: Maximum number of vertices to return
            return_keys: Keys to return
            unset_keys: Unused in TigerGraph

        Returns:
            list: Fetched vertices
        """
        filter_clause = ""
        if filters is not None:
            ff = Expression.from_dict(filters)
            filter_clause = f"WHERE {ff(doc_name='v', kind=DBFlavor.TIGERGRAPH)}"

        limit_clause = ""
        if limit is not None and isinstance(limit, int):
            limit_clause = f"LIMIT {limit}"

        return_clause = "v"
        if return_keys is not None:
            return_clause = f"{{ {', '.join([f'{k}: v.{k}' for k in return_keys])} }}"

        query = f"""
            SELECT {return_clause} FROM {class_name} v
            {filter_clause}
            {limit_clause}
        """
        
        try:
            result = self.execute(query)
            return result
        except Exception as e:
            logger.error(f"Error fetching vertices: {e}")
            return []

    def fetch_present_documents(
        self,
        batch,
        class_name,
        match_keys,
        keep_keys,
        flatten=False,
        filters: list | dict | None = None,
    ):
        """Fetch vertices that exist in the database.

        Note: Not implemented in TigerGraph.

        Args:
            batch: Batch of documents to check
            class_name: Vertex type to check in
            match_keys: Keys to match vertices
            keep_keys: Keys to keep in result
            flatten: Unused in TigerGraph
            filters: Additional query filters

        Raises:
            NotImplementedError: This method is not implemented for TigerGraph
        """
        raise NotImplementedError("fetch_present_documents not implemented for TigerGraph")

    def aggregate(
        self,
        class_name,
        aggregation_function: AggregationType,
        discriminant: str | None = None,
        aggregated_field: str | None = None,
        filters: list | dict | None = None,
    ):
        """Perform aggregation on vertices.

        Note: Not implemented in TigerGraph.

        Args:
            class_name: Vertex type to aggregate
            aggregation_function: Type of aggregation to perform
            discriminant: Field to group by
            aggregated_field: Field to aggregate
            filters: Query filters

        Raises:
            NotImplementedError: This method is not implemented for TigerGraph
        """
        raise NotImplementedError("aggregate not implemented for TigerGraph")

    def keep_absent_documents(
        self,
        batch,
        class_name,
        match_keys,
        keep_keys,
        filters: list | dict | None = None,
    ):
        """Keep vertices that don't exist in the database.

        Note: Not implemented in TigerGraph.

        Args:
            batch: Batch of documents to check
            class_name: Vertex type to check in
            match_keys: Keys to match vertices
            keep_keys: Keys to keep in result
            filters: Additional query filters

        Raises:
            NotImplementedError: This method is not implemented for TigerGraph
        """
        raise NotImplementedError("keep_absent_documents not implemented for TigerGraph")
