from test.db.neo4js.conftest import clean_db, conn_conf, test_db_name

import pytest

from graph_cast.db import ConnectionManager
from graph_cast.onto import InputType


@pytest.fixture(scope="function")
def modes():
    return [
        # "wos",
        # "lake_odds",
        # "kg_v3b",
        "review"
    ]


def test_ingest(
    ingest_atomic,
    clean_db,
    table_modes,
    conn_conf,
    current_path,
    test_db_name,
    reset,
):
    _ = clean_db
    for m in table_modes:
        ingest_atomic(
            conn_conf,
            current_path,
            test_db_name,
            input_type=InputType.CSV,
            mode=m,
        )
        if m == "review":
            # conn_conf.database = test_db_name
            with ConnectionManager(connection_config=conn_conf) as db_client:
                r = db_client.fetch_docs("Author")
                assert len(r) == 374
                r = db_client.fetch_docs(
                    "Author", filters=["==", "10", "hindex"]
                )
                assert len(r) == 8
                r = db_client.fetch_docs("Author", limit=1)
                assert len(r) == 1
                r = db_client.fetch_docs(
                    "Author",
                    filters=["==", "10", "hindex"],
                    return_keys=["full_name"],
                )
                assert len(r[0]) == 1
