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
            gsPort=config.gs_port,
            graphname=config.graphname,
            username=config.username,
            password=config.password,
            certPath=getattr(config, "certPath", None),
        )

        # Get authentication token if secret is provided
        if hasattr(config, "secret") and config.secret:
            try:
                self.conn.getToken(config.secret)
            except Exception as e:
                logger.warning(f"Failed to get authentication token: {e}")

    def create_database(self, name: str):
        """
        TigerGraph doesn't support creating graphs via API like ArangoDB.
        Graphs must be created manually or via GraphStudio/Admin Portal.
        """
        logger.info(
            f"TigerGraph doesn't support creating graphs via API. Graph '{name}' should be created manually."
        )

    def delete_database(self, name: str):
        """
        Clear all data from TigerGraph - graphs persist structurally.
        """
        try:
            # Clear all vertices (edges will be deleted automatically)
            vertex_types = self.conn.getVertexTypes()
            for v_type in vertex_types:
                result = self.conn.delVertices(v_type)
                logger.debug(f"Cleared vertices of type {v_type}: {result}")

        except Exception as e:
            logger.error(f"Could not clear database: {e}")

    def execute(self, query, **kwargs):
        """
        Execute GSQL query or installed query based on content.
        """
        try:
            # Check if this is an installed query call
            if query.strip().upper().startswith("RUN "):
                # Extract query name and parameters
                query_name = query.strip()[4:].split("(")[0].strip()
                result = self.conn.runInstalledQuery(query_name, **kwargs)
            else:
                # Execute as raw GSQL
                result = self.conn.gsql(query)
            return result
        except Exception as e:
            logger.error(f"Error executing query '{query}': {e}")
            raise

    def close(self):
        """Close connection - pyTigerGraph handles cleanup automatically."""
        pass

    def init_db(self, schema: Schema, clean_start=False):
        """
        Initialize database with schema definition.
        """
        if clean_start:
            self.delete_database("")

        try:
            # Define schema first, then create graph
            self.define_schema(schema)
            logger.info("Schema definition completed")
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise

    def define_schema(self, schema: Schema):
        """
        Define TigerGraph schema with proper GSQL syntax.
        """
        try:
            gsql_commands = []

            # Define vertex types
            for vertex in schema.vertex_config.vertices:
                # Map fields to proper TigerGraph types
                field_definitions = self._format_vertex_fields(vertex)

                gsql_command = f"""
                CREATE VERTEX {vertex.name} (
                    PRIMARY_ID id STRING,
                    {field_definitions}
                ) WITH STATS="OUTDEGREE_BY_EDGETYPE"
                """
                gsql_commands.append(gsql_command.strip())

            # Define edge types
            for edge in schema.edge_config.edges_list(include_aux=True):
                edge_attrs = self._format_edge_attributes(edge)

                gsql_command = f"""
                CREATE DIRECTED EDGE {edge.relation} (
                    FROM {edge.source},
                    TO {edge.target}
                    {edge_attrs}
                )
                """
                gsql_commands.append(gsql_command.strip())

            # Execute all schema commands
            for cmd in gsql_commands:
                logger.debug(f"Executing GSQL: {cmd}")
                result = self.conn.gsql(cmd)
                logger.debug(f"Result: {result}")

            # Create the graph after schema is defined
            self._create_graph_definition(schema)

        except Exception as e:
            logger.error(f"Error defining schema: {e}")
            raise

    def _format_vertex_fields(self, vertex_config) -> str:
        """
        Format vertex fields for GSQL CREATE VERTEX statement.
        """
        if hasattr(vertex_config, "fields") and vertex_config.fields:
            # If fields is already a string, return it
            if isinstance(vertex_config.fields, str):
                return vertex_config.fields
            # If fields is a dict, format it
            elif isinstance(vertex_config.fields, dict):
                field_list = []
                for field_name, field_type in vertex_config.fields.items():
                    tg_type = self._map_type_to_tigergraph(field_type)
                    field_list.append(f"{field_name} {tg_type}")
                return ",\n    ".join(field_list)

        # Default fields
        return """name STRING DEFAULT "",
    properties MAP<STRING, STRING> DEFAULT (map())"""

    def _format_edge_attributes(self, edge: Edge) -> str:
        """
        Format edge attributes for GSQL CREATE EDGE statement.
        """
        if hasattr(edge, "attributes") and edge.attributes:
            attrs = []
            for attr_name, attr_type in edge.attributes.items():
                tg_type = self._map_type_to_tigergraph(attr_type)
                attrs.append(f"{attr_name} {tg_type}")
            return ",\n    " + ",\n    ".join(attrs) if attrs else ""
        else:
            return ",\n    weight FLOAT DEFAULT 1.0"

    def _map_type_to_tigergraph(self, field_type: str) -> str:
        """
        Map common field types to TigerGraph types.
        """
        type_mapping = {
            "str": "STRING",
            "string": "STRING",
            "int": "INT",
            "integer": "INT",
            "float": "FLOAT",
            "double": "DOUBLE",
            "bool": "BOOL",
            "boolean": "BOOL",
            "datetime": "DATETIME",
            "date": "DATETIME",
        }
        return type_mapping.get(field_type.lower(), "STRING")

    def _create_graph_definition(self, schema: Schema):
        """
        Create the graph definition after vertices and edges are defined.
        """
        try:
            # Get graph name from schema or use default
            graph_name = getattr(schema, "graph_name", None)
            if not graph_name and schema.edge_config.edges_list():
                graph_name = getattr(
                    schema.edge_config.edges_list()[0], "graph_name", "DefaultGraph"
                )
            else:
                graph_name = "DefaultGraph"

            # Collect vertex and edge names
            vertex_list = [v.name for v in schema.vertex_config.vertices]
            edge_list = []
            for edge in schema.edge_config.edges_list(include_aux=True):
                edge_name = edge.relation or edge.collection_name
                if edge_name:
                    edge_list.append(edge_name)

            if vertex_list:
                vertex_str = ", ".join(vertex_list)
                edge_str = ", ".join(edge_list) if edge_list else ""

                if edge_str:
                    gsql_command = f"""
                    CREATE GRAPH {graph_name} (
                        {vertex_str},
                        {edge_str}
                    )
                    """
                else:
                    gsql_command = f"""
                    CREATE GRAPH {graph_name} ({vertex_str})
                    """

                logger.debug(f"Creating graph: {gsql_command}")
                result = self.conn.gsql(gsql_command.strip())
                logger.debug(f"Graph creation result: {result}")

        except Exception as e:
            logger.error(f"Error creating graph definition: {e}")
            # This might fail if graph already exists, which is often OK

    def define_vertex_collections(self, schema: Schema):
        """Vertex types are defined in schema, not as collections."""
        pass

    def define_edge_collections(self, edges: list[Edge]):
        """Edge types are defined in schema, not as collections."""
        pass

    def define_vertex_indices(self, vertex_config: VertexConfig):
        """
        TigerGraph automatically indexes primary keys.
        Secondary indices are less common but can be created.
        """
        for vertex_class in vertex_config.vertex_set:
            for index_obj in vertex_config.indexes(vertex_class):
                self._add_index(vertex_class, index_obj)

    def define_edge_indices(self, edges: list[Edge]):
        """Define indices for edges if specified."""
        for edge in edges:
            if hasattr(edge, "indexes"):
                for index_obj in edge.indexes:
                    if edge.relation:
                        self._add_index(edge.relation, index_obj, is_vertex_index=False)

    def _add_index(self, obj_name, index: Index, is_vertex_index=True):
        """
        Add index - TigerGraph has limited secondary index support.
        """
        try:
            # TigerGraph primarily uses primary key indexing
            # Secondary indices require special GSQL queries
            logger.info(
                f"Note: TigerGraph uses primary key indexing for {obj_name}. Secondary indices may require custom implementation."
            )
        except Exception as e:
            logger.warning(f"Could not create index for {obj_name}: {e}")

    def delete_collections(self, cnames=(), gnames=(), delete_all=False):
        """Delete vertex data (collections in TigerGraph terms)."""
        try:
            if cnames:
                for class_name in cnames:
                    result = self.conn.delVertices(class_name)
                    logger.debug(f"Deleted vertices from {class_name}: {result}")
            elif delete_all:
                vertex_types = self.conn.getVertexTypes()
                for v_type in vertex_types:
                    result = self.conn.delVertices(v_type)
                    logger.debug(f"Deleted all vertices from {v_type}: {result}")
        except Exception as e:
            logger.error(f"Error deleting collections: {e}")

    def upsert_docs_batch(self, docs, class_name, match_keys, **kwargs):
        """
        Batch upsert documents as vertices.
        """
        dry = kwargs.pop("dry", False)
        if dry:
            logger.debug(f"Dry run: would upsert {len(docs)} documents to {class_name}")
            return

        try:
            # Prepare vertices data for pyTigerGraph format
            vertices_data = []
            for doc in docs:
                vertex_id = self._extract_id(doc, match_keys)
                if vertex_id:
                    # Remove internal keys that shouldn't be stored
                    clean_doc = {
                        k: v
                        for k, v in doc.items()
                        if not k.startswith("_") or k == "_key"
                    }
                    vertices_data.append({vertex_id: clean_doc})

            # Batch upsert vertices
            if vertices_data:
                result = self.conn.upsertVertices(class_name, vertices_data)
                logger.debug(
                    f"Upserted {len(vertices_data)} vertices to {class_name}: {result}"
                )
                return result

        except Exception as e:
            logger.error(f"Error upserting vertices to {class_name}: {e}")
            # Fallback to individual operations
            self._fallback_individual_upsert(docs, class_name, match_keys)

    def _fallback_individual_upsert(self, docs, class_name, match_keys):
        """Fallback method for individual vertex upserts."""
        for doc in docs:
            try:
                vertex_id = self._extract_id(doc, match_keys)
                if vertex_id:
                    clean_doc = {
                        k: v
                        for k, v in doc.items()
                        if not k.startswith("_") or k == "_key"
                    }
                    self.conn.upsertVertex(class_name, vertex_id, clean_doc)
            except Exception as e:
                logger.error(f"Error upserting individual vertex {vertex_id}: {e}")

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
        Batch insert edges with proper error handling.
        """
        dry = kwargs.pop("dry", False)
        if dry:
            logger.debug(f"Dry run: would insert {len(docs_edges)} edges")
            return

        # Process edges list
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

                source_id = self._extract_id(source_doc, match_keys_source)
                target_id = self._extract_id(target_doc, match_keys_target)

                if source_id and target_id:
                    edges_data.append((source_id, target_id, edge_props))
                else:
                    logger.warning(
                        f"Missing source_id ({source_id}) or target_id ({target_id}) for edge"
                    )

            except Exception as e:
                logger.error(f"Error processing edge document: {e}")

        # Batch insert edges
        if edges_data:
            try:
                edge_type = relation_name or collection_name
                result = self.conn.upsertEdges(
                    source_class,
                    edge_type,
                    target_class,
                    edges_data,
                )
                logger.debug(
                    f"Inserted {len(edges_data)} edges of type {edge_type}: {result}"
                )
                return result
            except Exception as e:
                logger.error(f"Error batch inserting edges: {e}")

    def _extract_id(self, doc, match_keys):
        """
        Extract vertex ID from document based on match keys.
        """
        if not doc:
            return None

        # Try _key first (common in ArangoDB style docs)
        if "_key" in doc and doc["_key"]:
            return str(doc["_key"])

        # Try other match keys
        for key in match_keys:
            if key in doc and doc[key] is not None:
                return str(doc[key])

        # Fallback: create composite ID
        id_parts = []
        for key in match_keys:
            if key in doc and doc[key] is not None:
                id_parts.append(str(doc[key]))

        return "_".join(id_parts) if id_parts else None

    def insert_return_batch(self, docs, class_name):
        """
        TigerGraph doesn't have INSERT...RETURN semantics like ArangoDB.
        """
        raise NotImplementedError(
            "insert_return_batch not supported in TigerGraph - use upsert_docs_batch instead"
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
        Fetch documents (vertices) with filtering and projection.
        """
        try:
            # Get vertices using pyTigerGraph
            vertices = self.conn.getVertices(class_name, limit=limit)

            result = []
            for vertex_id, vertex_data in vertices.items():
                # Extract attributes
                attributes = vertex_data.get("attributes", {})
                doc = {**attributes, "_key": vertex_id}

                # Apply filters (client-side for now)
                if filters and not self._matches_filters(doc, filters):
                    continue

                # Apply projection
                if return_keys is not None:
                    doc = {k: doc.get(k) for k in return_keys if k in doc}
                elif unset_keys is not None:
                    doc = {k: v for k, v in doc.items() if k not in unset_keys}

                result.append(doc)

                # Apply limit after filtering
                if limit and len(result) >= limit:
                    break

            return result

        except Exception as e:
            logger.error(f"Error fetching documents from {class_name}: {e}")
            return []

    def _matches_filters(self, doc, filters):
        """Simple client-side filtering."""
        if isinstance(filters, dict):
            for key, value in filters.items():
                if doc.get(key) != value:
                    return False
        # For list filters, would need more complex logic
        return True

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
        Check which documents from batch are present in the database.
        """
        try:
            present_docs = {}

            for i, doc in enumerate(batch):
                vertex_id = self._extract_id(doc, match_keys)
                if not vertex_id:
                    continue

                try:
                    vertex_data = self.conn.getVerticesById(class_name, vertex_id)
                    if vertex_data and vertex_id in vertex_data:
                        # Extract requested keys
                        vertex_attrs = vertex_data[vertex_id].get("attributes", {})
                        filtered_doc = {}

                        for key in keep_keys:
                            if key == "_key":
                                filtered_doc[key] = vertex_id
                            elif key in vertex_attrs:
                                filtered_doc[key] = vertex_attrs[key]

                        present_docs[i] = [filtered_doc]

                except Exception:
                    # Vertex doesn't exist or error occurred
                    continue

            return present_docs

        except Exception as e:
            logger.error(f"Error fetching present documents: {e}")
            return {}

    def aggregate(
        self,
        class_name,
        aggregation_function: AggregationType,
        discriminant: str | None = None,
        aggregated_field: str | None = None,
        filters: list | dict | None = None,
    ):
        """
        Perform aggregation operations.
        """
        try:
            if aggregation_function == AggregationType.COUNT and discriminant is None:
                # Simple vertex count
                count = self.conn.getVertexCount(class_name)
                return [{"_value": count}]
            else:
                # Complex aggregations require custom GSQL queries
                logger.warning(
                    f"Complex aggregation {aggregation_function} requires custom GSQL implementation"
                )
                return []
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
        Return documents from batch that are NOT present in database.
        """
        present_docs_indices = self.fetch_present_documents(
            batch=batch,
            class_name=class_name,
            match_keys=match_keys,
            keep_keys=keep_keys,
            flatten=False,
            filters=filters,
        )

        absent_indices = sorted(
            set(range(len(batch))) - set(present_docs_indices.keys())
        )
        return [batch[i] for i in absent_indices]

    def define_indexes(self, schema: Schema):
        """Define all indexes from schema."""
        try:
            self.define_vertex_indices(schema.vertex_config)
            self.define_edge_indices(schema.edge_config.edges_list(include_aux=True))
        except Exception as e:
            logger.error(f"Error defining indexes: {e}")
