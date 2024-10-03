# from test.conftest import batch_edge_index
from test.conftest import fetch_schema_obj

import pytest

from graph_cast.caster import Caster
from graph_cast.db import ConnectionManager


@pytest.fixture(scope="function")
def modes():
    return [
        # "kg_v3b",
        "ibes",
        # "wos_json",
        # "lake_odds",
        # "wos",
        # "ticker",
    ]


# def test_ingest(
#     create_db,
#     modes,
#     conn_conf,
#     current_path,
#     test_db_name,
#     reset,
# ):
#     ingest_files(
#         create_db,
#         modes,
#         conn_conf,
#         current_path,
#         test_db_name,
#         reset,
#         n_cores=1,
#     )


def test_batch_edge_index(create_db, conn_conf, batch_edge_index, test_db_name):
    _ = create_db
    cschema = fetch_schema_obj("kg.edge.index")
    caster = Caster(cschema, n_threads=1, dry=False)
    conn_conf.database = test_db_name
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.init_db(cschema, True)

    caster.process_resource(
        batch_edge_index,
        resource_name="metrics_load",
        conn_conf=conn_conf,
    )

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # r_metrics = db_client.fetch_docs("metrics")
        r_edges = db_client.fetch_docs("publications_metrics_edges")

    assert len(r_edges) == 5
