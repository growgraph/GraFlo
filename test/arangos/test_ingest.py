import logging
import sys
from os.path import dirname, join, realpath
from pprint import pprint

import pytest
from suthing import FileHandle

from graph_cast.db import ConfigFactory, ConnectionManager
from graph_cast.main import ingest_json_files, ingest_tables
from graph_cast.util import ResourceHandler, equals

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
)


@pytest.fixture()
def current_path():
    return dirname(realpath(__file__))


@pytest.fixture(scope="function")
def test_db_name():
    return "testdb"


@pytest.fixture(scope="function")
def modes():
    return [
        # "wos",
        "lake_odds",
        "kg_v3b",
    ]


@pytest.fixture(scope="function")
def table_modes():
    return ["ibes", "wos", "ticker"]


@pytest.fixture(scope="function")
def conn_conf():
    db_args = {
        "protocol": "http",
        "ip_addr": "localhost",
        "port": 8535,
        "cred_name": "root",
        "database": "_system",
        "db_type": "arango",
    }
    cred_pass = FileHandle.load("docker.arango", "test.arango.secret")
    db_args["cred_pass"] = cred_pass
    conn_conf = ConfigFactory.create_config(args=db_args)
    return conn_conf


def json_atomic(conn_conf, current_path, test_db_name, mode):
    path = join(current_path, f"../data/json/{mode}")
    config = ResourceHandler.load(f"conf.json", f"{mode}.yaml")

    conn_conf.database = test_db_name
    ingest_json_files(
        path, config, conn_conf=conn_conf, ncores=1, upsert_option=False
    )


def table_atomic(conn_conf, current_path, test_db_name, mode):
    path = join(current_path, f"../data/table/{mode}")
    config = ResourceHandler.load(f"conf.table", f"{mode}.yaml")

    conn_conf.database = test_db_name
    ingest_tables(
        fpath=path,
        config=config,
        conn_conf=conn_conf,
        limit_files=None,
        clean_start=True,
    )


def verify(conn_conf, current_path, test_db_name, mode, ftype, reset=False):
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
            vc, join(current_path, f"../ref/{ftype}/{mode}_sizes.yaml")
        )
    else:
        ref_vc = ResourceHandler.load(
            f"test.ref.{ftype}", f"{mode}_sizes.yaml"
        )
        pprint(vc)
        pprint(ref_vc)
        assert equals(vc, ref_vc)


def test_json(modes, conn_conf, current_path, test_db_name, reset=False):
    for m in modes:
        json_atomic(conn_conf, current_path, test_db_name, mode=m)
        verify(
            conn_conf,
            current_path,
            test_db_name,
            mode=m,
            reset=reset,
            ftype="json",
        )


def test_csv(table_modes, conn_conf, current_path, test_db_name, reset=False):
    for m in table_modes:
        table_atomic(conn_conf, current_path, test_db_name, mode=m)
        verify(
            conn_conf,
            current_path,
            test_db_name,
            mode=m,
            reset=reset,
            ftype="table",
        )
