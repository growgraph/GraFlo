from test.db.arangos.conftest import ingest_files

import pytest


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
