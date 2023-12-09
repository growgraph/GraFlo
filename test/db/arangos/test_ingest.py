from os.path import join
from test.conftest import current_path, ingest_atomic, reset
from test.db.arangos.conftest import create_db, test_db_name

import pytest
from suthing import FileHandle, equals

from graph_cast.db import ConnectionManager
from graph_cast.onto import InputType


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
        collections = [c["name"] for c in cols if not c["system"]]
        for c in collections:
            cursor = db_client.execute(f"return LENGTH({c})")
            size = next(cursor)
            vc[c] = size
    if reset:
        FileHandle.dump(
            vc, join(current_path, f"../ref/{input_type}/{mode}_sizes.yaml")
        )
    else:
        ref_vc = FileHandle.load(
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
