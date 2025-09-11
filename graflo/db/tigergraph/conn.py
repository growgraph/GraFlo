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

import logging

from pyTigerGraph import TigerGraphConnection as PyTigerGraphConnection

from graflo.architecture.edge import Edge
from graflo.architecture.onto import Index
from graflo.architecture.schema import Schema
from graflo.architecture.vertex import VertexConfig
from graflo.db.conn import Connection
from graflo.db.connection.onto import TigergraphConnectionConfig
from graflo.onto import AggregationType, DBFlavor
from graflo.util.transform import pick_unique_dict

logger = logging.getLogger(__name__)


class TigerGraphConnection(Connection):
    """
    TigerGraph database connection implementation.

    Key conceptual differences from ArangoDB:
    1. TigerGraph uses GSQL (Graph Query Language) instead of AQL
    2. Schema must be defined explicitly before data insertion
    3. No automatic collection creation - vertices and edges must be pre-defined
    4. Different query syntax and execution model
    5. Token-based authentication for some operations
    """

    flavor = DBFlavor.TIGERGRAPH

    def __init__(self, config: TigergraphConnectionConfig):
        super().__init__()
        self.conn = PyTigerGraphConnection(
            host=config.url_without_port,
            restppPort=config.port,
            graphname=config.graphname,
            username=config.username,
            password=config.password,
            certPath=getattr(config, "certPath", None),
        )

        # Get authentication token if secret is provided
        # CONCEPTUAL DIFFERENCE: TigerGraph requires tokens for many operations
        if hasattr(config, "secret") and config.secret:
            self.conn.getToken(config.secret)

    def create_database(self, name: str):
        """
        CONCEPTUAL DIFFERENCE: TigerGraph doesn't support creating graphs via API
        like ArangoDB creates databases. Graphs must be created manually or via
        GraphStudio/Admin Portal.
        """
        logger.info(
            f"TigerGraph doesn't support creating graphs via API. Graph '{name}' should be created manually."
        )

    def delete_database(self, name: str):
        """
        CONCEPTUAL DIFFERENCE: Instead of deleting the entire graph,
        we clear all data. TigerGraph graphs persist structurally.
        """
        try:
            # Get all vertex types and clear them
            vertex_types = self.conn.getVertexTypes()
            for v_type in vertex_types:
                self.conn.delVertices(v_type)

            # Clear all edges
            edge_types = self.conn.getEdgeTypes()
            for e_type in edge_types:
                # TigerGraph doesn't have a direct "delete all edges of type"
                # so we'd need to implement this differently
                pass

        except Exception as e:
            logger.error(f"Could not clear database: {e}")

    def execute(self, query, **kwargs):
        """
        CONCEPTUAL DIFFERENCE: TigerGraph has different query execution methods
        - runInstalledQuery() for pre-installed queries
        - gsql() for direct GSQL execution
        - Various specific methods for common operations
        """
        try:
            # Check if this is an installed query or raw GSQL
            if query.startswith("RUN ") or not any(
                keyword in query.upper()
                for keyword in ["CREATE", "DROP", "ALTER", "INSTALL"]
            ):
                # Assume it's an installed query name
                result = self.conn.runInstalledQuery(query, **kwargs)
            else:
                # Execute as raw GSQL
                result = self.conn.gsql(query)
            return result
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise

    def close(self):
        pass

    def init_db(self, schema: Schema, clean_start):
        """
        CONCEPTUAL DIFFERENCE: TigerGraph requires explicit schema definition
        before any data operations, unlike ArangoDB's dynamic collections.
        """
        if clean_start:
            self.delete_database("")

        # TigerGraph requires schema to be defined first
        self.define_schema(schema)
        self.define_indexes(schema)

    def define_schema(self, schema: Schema):
        """
        CONCEPTUAL DIFFERENCE: TigerGraph requires explicit vertex and edge type
        definitions before data can be inserted. This is a major difference from
        ArangoDB which creates collections dynamically.
        """
        try:
            # Define vertex types
            for v in schema.vertex_config.vertices:
                #  PRIMARY_ID title STRING, tagline STRING, released DATETIME
                # INT, UINT, FLOAT, DOUBLE, STRING, BOOL, DATETIME, VERTEX, EDGE
                gsql_command = f"""
                CREATE VERTEX {v.name} (
                    PRIMARY_ID id STRING,
                    {v.fields}
                ) WITH STATS="OUTDEGREE_BY_EDGETYPE"
                """
                self.conn.gsql(gsql_command)

            # Define edge types
            for edge in schema.edge_config.edges_list(include_aux=True):
                edge_attrs = self._get_edge_attributes(edge)
                # CREATE UNDIRECTED EDGE ACTED_IN (FROM person, TO movie)
                gsql_command = f"""
                CREATE DIRECTED EDGE {edge.relation} (
                    FROM {edge.source},
                    TO {edge.target}
                    {edge_attrs}
                ) WITH REVERSE_EDGE="{edge.relation}_reverse"
                """
                self.conn.gsql(gsql_command)

            # Create the graph
            graph_name = (
                schema.edge_config.edges_list()[0].graph_name
                if schema.edge_config.edges_list()
                else "DefaultGraph"
            )
            vertex_list = ", ".join(schema.vertex_config.vertex_set)
            edge_list = ", ".join(
                [
                    e.relation or e.collection_name
                    for e in schema.edge_config.edges_list()
                ]
            )

            gsql_command = f"""
            CREATE GRAPH {graph_name} (
                {vertex_list},
                {edge_list}
            )
            """
            self.conn.gsql(gsql_command)

        except Exception as e:
            logger.error(f"Error defining schema: {e}")

    def _get_vertex_attributes(
        self, vertex_config: VertexConfig, vertex_class: str
    ) -> str:
        """
        Helper to extract vertex attributes from schema.
        CONCEPTUAL DIFFERENCE: TigerGraph needs explicit type definitions.
        """
        # This would need to be implemented based on your schema structure
        # For now, return common attributes
        return """
            name STRING DEFAULT "",
            properties MAP<STRING, STRING> DEFAULT (map())
        """

    def _get_edge_attributes(self, edge: Edge) -> str:
        """
        Helper to extract edge attributes from schema.
        """
        if hasattr(edge, "attributes") and edge.attributes:
            attrs = []
            for attr_name, attr_type in edge.attributes.items():
                attrs.append(f'{attr_name} {attr_type} DEFAULT ""')
            return ",\n    " + ",\n    ".join(attrs) if attrs else ""
        else:
            return ",\n    weight FLOAT DEFAULT 1.0"

    def define_vertex_collections(self, schema: Schema):
        """
        CONCEPTUAL DIFFERENCE: Vertex types are defined in schema, not as collections.
        """
        pass

    def define_edge_collections(self, edges: list[Edge]):
        """
        CONCEPTUAL DIFFERENCE: Edge types are defined in schema, not as collections.
        """
        pass

    def define_vertex_indices(self, vertex_config: VertexConfig):
        """
        CONCEPTUAL DIFFERENCE: TigerGraph automatically indexes primary keys.
        Additional indices require different GSQL syntax.
        """
        for c in vertex_config.vertex_set:
            for index_obj in vertex_config.indexes(c):
                self._add_index(c, index_obj)

    def define_edge_indices(self, edges: list[Edge]):
        for edge in edges:
            for index_obj in edge.indexes:
                if edge.relation is not None:
                    self._add_index(edge.relation, index_obj, is_vertex_index=False)

    def _add_index(self, obj_name, index: Index, is_vertex_index=True):
        """
        CONCEPTUAL DIFFERENCE: TigerGraph index creation syntax differs from ArangoDB.
        Secondary indices are less commonly used in TigerGraph.
        """
        try:
            # TigerGraph doesn't support arbitrary secondary indices like ArangoDB
            # Most queries are optimized through primary key access
            logger.info(
                f"Secondary indices not commonly used in TigerGraph for {obj_name}"
            )
        except Exception as e:
            logger.warning(f"Could not create index for {obj_name}: {e}")

    def delete_collections(self, cnames=(), gnames=(), delete_all=False):
        """
        CONCEPTUAL DIFFERENCE: TigerGraph deletes data, not schema structures.
        """
        try:
            if cnames:
                for c in cnames:
                    self.conn.delVertices(c)
            elif delete_all:
                vertex_types = self.conn.getVertexTypes()
                for v_type in vertex_types:
                    self.conn.delVertices(v_type)
        except Exception as e:
            logger.error(f"Error deleting collections: {e}")

    def upsert_docs_batch(self, docs, class_name, match_keys, **kwargs):
        """
        CONCEPTUAL DIFFERENCE: TigerGraph uses different upsert syntax and
        the pyTigerGraph library provides higher-level methods.
        """
        dry = kwargs.pop("dry", False)

        if dry:
            return

        try:
            # Use pyTigerGraph's upsertVertices method which is more efficient
            # than individual GSQL queries
            vertices_data = []
            for doc in docs:
                # TigerGraph requires explicit primary ID
                vertex_id = doc.get("_key") or doc.get("id") or str(hash(str(doc)))
                vertex_data = {vertex_id: doc}
                vertices_data.append(vertex_data)

            # Batch upsert vertices
            if vertices_data:
                result = self.conn.upsertVertices(class_name, vertices_data)
                logger.debug(f"Upserted {len(vertices_data)} vertices: {result}")

        except Exception as e:
            logger.error(f"Error upserting vertices: {e}")
            # Fallback to individual operations
            for doc in docs:
                try:
                    vertex_id = doc.get("_key") or doc.get("id") or str(hash(str(doc)))
                    self.conn.upsertVertex(class_name, vertex_id, doc)
                except Exception as inner_e:
                    logger.error(f"Error upserting individual vertex: {inner_e}")

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
        """
        CONCEPTUAL DIFFERENCE: TigerGraph edge insertion uses different methods
        and doesn't have the same flexible matching as ArangoDB's AQL.
        """
        dry = kwargs.pop("dry", False)

        if dry:
            return

        if isinstance(docs_edges, list):
            if head is not None:
                docs_edges = docs_edges[:head]
            if filter_uniques:
                docs_edges = pick_unique_dict(docs_edges)

        edges_data = []
        for edge_doc in docs_edges:
            try:
                source_doc = edge_doc.get("_source_aux", {})
                target_doc = edge_doc.get("_target_aux", {})
                edge_props = edge_doc.get("_edge_props", {})

                # Extract source and target IDs based on match keys
                source_id = self._extract_id(source_doc, match_keys_source)
                target_id = self._extract_id(target_doc, match_keys_target)

                if source_id and target_id:
                    edge_data = (source_id, target_id, edge_props)
                    edges_data.append(edge_data)

            except Exception as e:
                logger.error(f"Error processing edge document: {e}")

        # Batch insert edges
        if edges_data:
            try:
                result = self.conn.upsertEdges(
                    source_class,
                    relation_name or collection_name,
                    target_class,
                    edges_data,
                )
                logger.debug(f"Inserted {len(edges_data)} edges: {result}")
            except Exception as e:
                logger.error(f"Error batch inserting edges: {e}")

    def _extract_id(self, doc, match_keys):
        """
        Helper to extract ID from document based on match keys.
        CONCEPTUAL DIFFERENCE: TigerGraph requires explicit vertex IDs.
        """
        if "_key" in match_keys and "_key" in doc:
            return doc["_key"]
        elif "id" in match_keys and "id" in doc:
            return doc["id"]
        elif len(match_keys) == 1 and match_keys[0] in doc:
            return str(doc[match_keys[0]])
        else:
            # Fallback: create ID from all match key values
            id_parts = [str(doc.get(key, "")) for key in match_keys if key in doc]
            return "_".join(id_parts) if id_parts else None

    def insert_return_batch(self, docs, class_name):
        """
        CONCEPTUAL DIFFERENCE: TigerGraph doesn't have the same return semantics
        as ArangoDB's INSERT...RETURN. We'd need to track inserted IDs manually.
        """
        raise NotImplementedError(
            "insert_return_batch not implemented for TigerGraph - use upsert_docs_batch instead"
        )

    def fetch_docs(
        self,
        class_name,
        filters: list | dict | None = None,
        limit: int | None = None,
        return_keys: list | None = None,
        unset_keys: list | None = None,
    ):
        """
        CONCEPTUAL DIFFERENCE: TigerGraph uses installed queries or GSQL SELECT
        statements instead of AQL-style queries. This requires a different approach.
        """
        try:
            # Use pyTigerGraph's getVertices method which is more efficient
            if filters is None:
                # Get all vertices of this type
                vertices = self.conn.getVertices(class_name, limit=limit)
            else:
                # For filtered queries, we'd need to use GSQL or installed queries
                # This is a simplified implementation
                vertices = self.conn.getVertices(class_name, limit=limit)
                # Apply filtering client-side (not optimal, but works for simple cases)
                if isinstance(filters, dict):
                    filtered_vertices = []
                    for v_id, v_data in vertices.items():
                        match = True
                        for key, value in filters.items():
                            if v_data.get("attributes", {}).get(key) != value:
                                match = False
                                break
                        if match:
                            filtered_vertices.append(
                                {**v_data["attributes"], "_key": v_id}
                            )
                    return filtered_vertices[:limit] if limit else filtered_vertices

            # Convert to list format similar to ArangoDB
            result = []
            for v_id, v_data in vertices.items():
                doc = {**v_data.get("attributes", {}), "_key": v_id}

                # Apply return_keys filtering
                if return_keys is not None:
                    doc = {k: doc.get(k) for k in return_keys if k in doc}
                elif unset_keys is not None:
                    doc = {k: v for k, v in doc.items() if k not in unset_keys}

                result.append(doc)

            return result[:limit] if limit else result

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
        """
        CONCEPTUAL DIFFERENCE: TigerGraph requires different approach for checking
        document presence. We need to query by specific vertex IDs.
        """
        try:
            present_docs = {}

            for i, doc in enumerate(batch):
                vertex_id = self._extract_id(doc, match_keys)
                if vertex_id:
                    try:
                        vertex_data = self.conn.getVerticesById(class_name, vertex_id)
                        if vertex_data:
                            # Extract only the keys we want to keep
                            filtered_doc = {}
                            vertex_attrs = vertex_data[vertex_id]["attributes"]
                            for key in keep_keys:
                                if key in vertex_attrs:
                                    filtered_doc[key] = vertex_attrs[key]
                                elif key == "_key":
                                    filtered_doc[key] = vertex_id

                            if flatten:
                                present_docs[i] = [filtered_doc]
                            else:
                                present_docs[i] = [filtered_doc]
                    except:
                        # Vertex doesn't exist
                        continue

            return present_docs

        except Exception as e:
            logger.error(f"Error fetching present documents: {e}")
            return {} if not flatten else []

    def aggregate(
        self,
        class_name,
        aggregation_function: AggregationType,
        discriminant: str | None = None,
        aggregated_field: str | None = None,
        filters: list | dict | None = None,
    ):
        """
        CONCEPTUAL DIFFERENCE: TigerGraph aggregations typically require
        installed queries for complex operations. Simple counts can be done
        with built-in methods.
        """
        try:
            if aggregation_function == AggregationType.COUNT and discriminant is None:
                # Simple count
                count = self.conn.getVertexCount(class_name)
                return [{"_value": count}]
            else:
                # Complex aggregations require installed queries in TigerGraph
                raise NotImplementedError(
                    f"Complex aggregation {aggregation_function} not implemented - requires custom GSQL query"
                )
        except Exception as e:
            logger.error(f"Error in aggregation: {e}")
            return []

    def keep_absent_documents(
        self,
        batch,
        class_name,
        match_keys,
        keep_keys,
        filters: list | dict | None = None,
    ):
        """
        Similar logic to ArangoDB but using TigerGraph's vertex checking methods.
        """
        present_docs_keys = self.fetch_present_documents(
            batch=batch,
            class_name=class_name,
            match_keys=match_keys,
            keep_keys=keep_keys,
            flatten=False,
            filters=filters,
        )

        assert isinstance(present_docs_keys, dict)

        absent_indices = sorted(set(range(len(batch))) - set(present_docs_keys.keys()))
        batch_absent = [batch[j] for j in absent_indices]
        return batch_absent
