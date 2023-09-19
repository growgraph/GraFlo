from os.path import join
from pprint import pprint
from test.arangos.conftest import create_db
from test.conftest import current_path, ingest_atomic, reset

import pytest

from graph_cast.db import ConnectionManager
from graph_cast.onto import InputType
from graph_cast.util import ResourceHandler, equals


@pytest.fixture(scope="function")
def modes():
    return [
        # "wos_json",
        "lake_odds",
        "kg_v3b",
    ]


@pytest.fixture(scope="function")
def table_modes():
    return [
        "ibes",
        # "wos",
        "ticker",
    ]


def verify(
    conn_conf, current_path, test_db_name, mode, input_type: InputType, reset
):
    conn_conf.database = test_db_name
    with ConnectionManager(connection_config=conn_conf) as db_client:
        cols = db_client.get_collections()
        vc = {}
        for c in cols:
            if not c["system"]:
                cursor = db_client.execute(f"return LENGTH({c['name']})")
                size = next(cursor)
                vc[c["name"]] = size
    if reset:
        ResourceHandler.dump(
            vc, join(current_path, f"../ref/{input_type}/{mode}_sizes.yaml")
        )
    else:
        ref_vc = ResourceHandler.load(
            f"test.ref.{input_type}", f"{mode}_sizes.yaml"
        )
        if not equals(vc, ref_vc):
            print(f" mode: {mode}")
            for k, v in ref_vc.items():
                print(
                    f" {k} expected: {v}, received:"
                    f" {vc[k] if k in vc else None}"
                )
        assert equals(vc, ref_vc)


def test_json(create_db, modes, conn_conf, current_path, test_db_name, reset):
    _ = create_db
    for m in modes:
        ingest_atomic(
            conn_conf,
            current_path,
            test_db_name,
            input_type=InputType.JSON,
            mode=m,
        )
        verify(
            conn_conf,
            current_path,
            test_db_name,
            mode=m,
            reset=reset,
            input_type=InputType.JSON,
        )


def test_csv(
    create_db, table_modes, conn_conf, current_path, test_db_name, reset
):
    _ = create_db

    for m in table_modes:
        ingest_atomic(
            conn_conf,
            current_path,
            test_db_name,
            input_type=InputType.TABLE,
            mode=m,
        )
        verify(
            conn_conf,
            current_path,
            test_db_name,
            mode=m,
            reset=reset,
            input_type=InputType.TABLE,
        )
