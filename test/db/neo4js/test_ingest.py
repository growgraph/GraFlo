from test.conftest import current_path, ingest_atomic, reset
from test.db.neo4js.conftest import clean_db, conn_conf, test_db_name

import pytest

from graph_cast.onto import InputType


@pytest.fixture(scope="function")
def modes():
    return [
        # "wos",
        # "lake_odds",
        # "kg_v3b",
    ]


@pytest.fixture(scope="function")
def table_modes():
    return ["review"]


def test_csv(
    clean_db, table_modes, conn_conf, current_path, test_db_name, reset
):
    _ = clean_db
    for m in table_modes:
        ingest_atomic(
            conn_conf,
            current_path,
            test_db_name,
            input_type=InputType.TABLE,
            mode=m,
        )
        # verify(
        #     conn_conf,
        #     current_path,
        #     test_db_name,
        #     mode=m,
        #     reset=reset,
        #     input_type=InputType.TABLE,
        # )
