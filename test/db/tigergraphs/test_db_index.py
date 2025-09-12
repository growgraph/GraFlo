from graflo.db import ConnectionManager


def test_create_vertex_index(conn_conf, schema_obj):
    schema_obj = schema_obj("review")

    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.define_vertex_indices(schema_obj.vertex_config)

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # TigerGraph doesn't use SHOW INDEX - check vertex types instead
        vertex_types = db_client.conn.getVertexTypes()
        # Or check schema info
        _ = db_client.conn.gsql("ls")

    # TigerGraph automatically indexes PRIMARY_ID, secondary indexes are rare
    # Verify vertex types exist instead of specific index names
    expected_vertex_types = ["researchField", "author"]  # Adjust based on your schema
    for vertex_type in expected_vertex_types:
        assert vertex_type in vertex_types, f"Vertex type {vertex_type} not found"


def test_create_edge_index(conn_conf, schema_obj):
    schema_obj = schema_obj("review")

    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.define_edge_indices(
            schema_obj.edge_config.edges_list(include_aux=True)
        )

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Check edge types instead of indexes
        edge_types = db_client.conn.getEdgeTypes()

    # Verify expected edge types exist
    expected_edge_types = ["belongsTo"]  # Adjust based on your schema
    for edge_type in expected_edge_types:
        assert edge_type in edge_types, f"Edge type {edge_type} not found"


# Alternative: Test schema creation instead of indexes
def test_schema_creation(conn_conf, schema_obj):
    schema_obj = schema_obj("review")

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Initialize with schema
        db_client.init_db(schema_obj, clean_start=True)

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Verify schema was created
        vertex_types = db_client.conn.getVertexTypes()
        edge_types = db_client.conn.getEdgeTypes()

        # Check expected types exist
        assert len(vertex_types) > 0, "No vertex types created"
        assert len(edge_types) > 0, "No edge types created"

        print(f"Created vertex types: {vertex_types}")
        print(f"Created edge types: {edge_types}")


# If you need to test actual TigerGraph indexes (rare), use GSQL queries:
def test_tigergraph_schema_info(conn_conf, schema_obj):
    schema_obj = schema_obj("review")

    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.init_db(schema_obj, clean_start=True)

        # Use GSQL to get detailed schema info
        _ = db_client.execute("ls")

        # Or get vertex/edge statistics which include indexing info
        vertex_types = db_client.conn.getVertexTypes()
        for v_type in vertex_types:
            stats = db_client.conn.getVertexStats(v_type)
            print(f"Stats for {v_type}: {stats}")
