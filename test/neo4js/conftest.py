import os
from os.path import dirname, realpath

import pytest
from suthing import FileHandle

from graph_cast.db import ConfigFactory
from graph_cast.main import ingest_files
from graph_cast.util import ResourceHandler


@pytest.fixture(scope="function")
def test_db_port():
    env = FileHandle.load("docker.neo4j", ".env")
    port = os.environ["NEO4J_BOLT_PORT"]
    return port


@pytest.fixture(scope="function")
def creds():
    env = FileHandle.load("docker.neo4j", ".env")
    creds = os.environ["NEO4J_AUTH"].split("/")
    cred_name, cred_pass = creds[0], creds[1]
    return cred_name, cred_pass


@pytest.fixture(scope="function")
def conn_conf(test_db_port, creds):
    cred_name, cred_pass = creds

    db_args = {
        "protocol": "bolt",
        "ip_addr": "localhost",
        "cred_name": cred_name,
        "cred_pass": cred_pass,
        "port": test_db_port,
        "database": "_system",
        "db_type": "arango",
    }
    conn_conf = ConfigFactory.create_config(dict_like=db_args)
    return conn_conf


@pytest.fixture()
def current_path():
    return dirname(realpath(__file__))


# def ingest_atomic(conn_conf, current_path, test_db_name, input_type, mode):
#     path = join(current_path, f"../data/{input_type}/{mode}")
#     schema = ResourceHandler.load(f"conf.{input_type}", f"{mode}.yaml")
#
#     conn_conf.database = test_db_name
#     ingest_files(
#         fpath=path,
#         schema=schema,
#         conn_conf=conn_conf,
#         input_type=input_type,
#         limit_files=None,
#         clean_start=True,
#     )
