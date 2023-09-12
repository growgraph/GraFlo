import os

import pytest
from suthing import FileHandle

from graph_cast.db import ConfigFactory


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
