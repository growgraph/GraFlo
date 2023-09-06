import os
from os.path import dirname, join, realpath

import pytest
from suthing import FileHandle

from graph_cast.db import ConfigFactory
from graph_cast.main import ingest_files


@pytest.fixture(scope="function")
def test_db_name():
    return "testdb"


@pytest.fixture(scope="function")
def test_db_port():
    env = FileHandle.load("docker.arango", ".env")
    port = os.environ["ARANGO_PORT"]
    return port


@pytest.fixture(scope="function")
def conn_conf(test_db_port):
    cred_pass = FileHandle.load("docker.arango", "test.arango.secret")

    db_args = {
        "protocol": "http",
        "ip_addr": "localhost",
        "port": test_db_port,
        "cred_name": "root",
        "cred_pass": cred_pass,
        "database": "_system",
        "db_type": "arango",
    }

    conn_conf = ConfigFactory.create_config(dict_like=db_args)
    return conn_conf


@pytest.fixture()
def current_path():
    return dirname(realpath(__file__))


def ingest_atomic(conn_conf, current_path, test_db_name, input_type, mode):
    path = join(current_path, f"../data/{input_type}/{mode}")
    schema = FileHandle.load(f"conf.{input_type}", f"{mode}.yaml")

    conn_conf.database = test_db_name
    ingest_files(
        fpath=path,
        schema=schema,
        conn_conf=conn_conf,
        input_type=input_type,
        limit_files=None,
        clean_start=True,
    )
