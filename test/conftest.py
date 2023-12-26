from os.path import dirname, join, realpath

import pytest
from suthing import FileHandle

from graph_cast.architecture.schema import Schema
from graph_cast.main import ingest_files
from graph_cast.onto import InputTypeFileExtensions


def pytest_addoption(parser):
    parser.addoption("--reset", action="store_true")


@pytest.fixture(scope="session", autouse=True)
def reset(request):
    return request.config.getoption("reset")


@pytest.fixture(scope="function")
def current_path():
    return dirname(realpath(__file__))


def fetch_schema_dict(mode):
    schema_dict = FileHandle.load(f"test.config.schema", f"{mode}.yaml")
    return schema_dict


def fetch_schema_obj(mode):
    schema_obj = Schema.from_dict(fetch_schema_dict(mode))
    return schema_obj


@pytest.fixture(scope="function")
def schema():
    return fetch_schema_dict


@pytest.fixture(scope="function")
def schema_obj():
    return fetch_schema_obj


@pytest.fixture(scope="function")
def ingest_atomic():
    def ingest_atomic_(conn_conf, current_path, test_db_name, mode):
        schema_o = fetch_schema_obj(mode)
        rr = schema_o.fetch_resource()
        path = join(
            current_path,
            f"data.{InputTypeFileExtensions[rr.resource_type][0]}.{mode}",
        )

        conn_conf.database = test_db_name
        ingest_files(
            fpath=path,
            schema=schema_o,
            conn_conf=conn_conf,
            limit_files=None,
            clean_start=True,
        )

    return ingest_atomic_
