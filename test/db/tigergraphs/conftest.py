import os

import pytest
from suthing import FileHandle

from graflo.db import ConfigFactory, ConnectionManager


@pytest.fixture(scope="function")
def test_db_port():
    FileHandle.load("docker.tigergraph", ".env")
    port = os.environ["TG_REST"]
    return port


@pytest.fixture(scope="function")
def test_gs_port():
    FileHandle.load("docker.tigergraph", ".env")
    port = os.environ["TG_WEB"]
    return port


@pytest.fixture(scope="function")
def creds():
    FileHandle.load("docker.tigergraph", ".env")
    cred_pass = os.environ["GSQL_PASSWORD"]
    cred_name = "tigergraph"
    return cred_name, cred_pass


@pytest.fixture(scope="function")
def conn_conf(test_db_port, test_gs_port, creds):
    cred_name, cred_pass = creds

    db_args = {
        "protocol": "http",
        "hostname": "localhost",
        "cred_name": cred_name,
        "cred_pass": cred_pass,
        "port": test_db_port,
        "gs_port": test_gs_port,
        # "database": "_system",
        "db_type": "tigergraph",
    }
    conn_conf = ConfigFactory.create_config(db_args)
    return conn_conf


@pytest.fixture()
def clean_db(conn_conf):
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.delete_collections()


@pytest.fixture(scope="function")
def test_db_name():
    return "tigergraph"
