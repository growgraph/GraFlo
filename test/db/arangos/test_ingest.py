from test.conftest import ingest_atomic, verify

import pytest

from graph_cast.db import ConnectionManager
from graph_cast.onto import AggregationType, ComparisonOperator


@pytest.fixture(scope="function")
def modes():
    return [
        "kg_v3b",
        "ibes",
        # "wos_json",
        # "lake_odds",
        # "wos",
        # "ticker",
    ]


def verify_from_db(conn_conf, current_path, test_db_name, mode, reset):
    conn_conf.database = test_db_name
    with ConnectionManager(connection_config=conn_conf) as db_client:
        cols = db_client.get_collections()
        vc = {}
        collections = [c["name"] for c in cols if not c["system"]]
        for c in collections:
            cursor = db_client.execute(f"return LENGTH({c})")
            size = next(cursor)
            vc[c] = size

    verify(vc, current_path, mode, test_type="db", reset=reset)


def ingest_files(
    create_db, modes, conn_conf, current_path, test_db_name, reset, n_cores=1
):
    _ = create_db
    for m in modes:
        ingest_atomic(
            conn_conf, current_path, test_db_name, mode=m, n_cores=n_cores
        )
        verify_from_db(
            conn_conf,
            current_path,
            test_db_name,
            mode=m,
            reset=reset,
        )
        if m == "lake_odds":
            conn_conf.database = test_db_name
            with ConnectionManager(connection_config=conn_conf) as db_client:
                r = db_client.fetch_docs("chunks")
                assert len(r) == 2
                assert r[0]["data"]
                r = db_client.fetch_docs(
                    "chunks", filters=["==", "odds", "kind"]
                )
                assert len(r) == 1
                r = db_client.fetch_docs("chunks", limit=1)
                assert len(r) == 1
                r = db_client.fetch_docs(
                    "chunks",
                    filters=["==", "odds", "kind"],
                    return_keys=["kind"],
                )
                assert len(r[0]) == 1
            batch = [{"kind": "odds"}, {"kind": "strange"}]
            with ConnectionManager(connection_config=conn_conf) as db_client:
                r = db_client.fetch_present_documents(
                    batch,
                    "chunks",
                    match_keys=("kind",),
                    keep_keys=("_key",),
                    flatten=False,
                )
                assert len(r) == 1

            batch = [{"kind": "odds"}, {"kind": "scores"}, {"kind": "strange"}]
            with ConnectionManager(connection_config=conn_conf) as db_client:
                r = db_client.fetch_present_documents(
                    batch,
                    "chunks",
                    match_keys=("kind",),
                    keep_keys=("_key",),
                    flatten=False,
                    filters=[ComparisonOperator.NEQ, "odds", "kind"],
                )
                assert len(r) == 1

            with ConnectionManager(connection_config=conn_conf) as db_client:
                r = db_client.keep_absent_documents(
                    batch,
                    "chunks",
                    match_keys=("kind",),
                    keep_keys=("_key",),
                    filters=[ComparisonOperator.EQ, None, "data"],
                )
                assert len(r) == 3

            with ConnectionManager(connection_config=conn_conf) as db_client:
                r = db_client.aggregate(
                    "chunks",
                    aggregation_function=AggregationType.COUNT,
                    discriminant="kind",
                )
                assert len(r) == 2
                assert r == [
                    {"kind": "odds", "_value": 1},
                    {"kind": "scores", "_value": 1},
                ]

            with ConnectionManager(connection_config=conn_conf) as db_client:
                r = db_client.aggregate(
                    "chunks",
                    aggregation_function=AggregationType.COUNT,
                    discriminant="kind",
                    filters=[ComparisonOperator.NEQ, "odds", "kind"],
                )
                assert len(r) == 1


def test_ingest(
    create_db,
    modes,
    conn_conf,
    current_path,
    test_db_name,
    reset,
):
    ingest_files(
        create_db,
        modes,
        conn_conf,
        current_path,
        test_db_name,
        reset,
        n_cores=1,
    )

    ingest_files(
        create_db,
        modes,
        conn_conf,
        current_path,
        test_db_name,
        reset,
        n_cores=2,
    )
