from test.conftest import ingest_atomic, verify

import pytest

from graph_cast.db import ConnectionManager


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


def init_db(m, conn_conf, schema, current_path, reset):
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.init_db(schema, clean_start=True)
        ixs = db_client.fetch_indexes()

    for k, batch in ixs.items():
        for ix in batch:
            del ix["id"]
            del ix["name"]
            _ = ix.pop("selectivity", None)

    verify(ixs, current_path, m, test_type="db", kind="indexes", reset=reset)


# @pytest.mark.skip(reason="no way of currently testing this")
def test_index(
    create_db, modes, conn_conf, schema_obj, current_path, test_db_name, reset
):
    _ = create_db
    conn_conf.database = test_db_name
    for m in modes:
        init_db(m, conn_conf, schema_obj(m), current_path, reset)
