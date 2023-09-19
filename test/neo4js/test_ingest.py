from test.conftest import ingest_atomic

import pytest

from graph_cast.db import ConnectionManager
from graph_cast.onto import InputType
from graph_cast.util import ResourceHandler, equals


@pytest.fixture(scope="function")
def modes():
    return [
        # "wos",
        "lake_odds",
        "kg_v3b",
    ]


@pytest.fixture(scope="function")
def table_modes():
    return ["review"]


# def test_csv(table_modes, conn_conf, current_path, test_db_name, reset):
#     for m in table_modes:
#         ingest_atomic(
#             conn_conf,
#             current_path,
#             test_db_name,
#             input_type=InputType.TABLE,
#             mode=m,
#         )
#         verify(
#             conn_conf,
#             current_path,
#             test_db_name,
#             mode=m,
#             reset=reset,
#             input_type=InputType.TABLE,
#         )
