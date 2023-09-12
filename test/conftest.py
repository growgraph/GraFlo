from os.path import dirname, join, realpath

import pytest
from suthing import FileHandle

from graph_cast.main import ingest_files


def pytest_addoption(parser):
    parser.addoption("--reset", action="store_true")


@pytest.fixture(scope="session", autouse=True)
def reset(request):
    return request.config.getoption("reset")


@pytest.fixture(scope="function")
def current_path():
    return dirname(realpath(__file__))


def ingest_atomic(conn_conf, current_path, test_db_name, input_type, mode):
    path = join(current_path, f"data/{input_type}/{mode}")
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
