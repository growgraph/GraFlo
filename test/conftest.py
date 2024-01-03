from os.path import dirname, join, realpath
from pathlib import Path

import pytest
from suthing import FileHandle, equals

from graph_cast.architecture.onto import cast_graph_name_to_triple
from graph_cast.architecture.schema import Schema
from graph_cast.caster import Caster
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


def ingest_atomic(conn_conf, current_path, test_db_name, mode):
    schema_o = fetch_schema_obj(mode)
    rr = schema_o.fetch_resource()
    path = Path(
        join(
            current_path,
            f"data/{InputTypeFileExtensions[rr.resource_type][0]}/{mode}",
        )
    )

    conn_conf.database = test_db_name

    caster = Caster(schema_o)
    caster.ingest_files(
        path=path,
        limit_files=None,
        clean_start=True,
        conn_conf=conn_conf,
    )


def verify(vc, current_path, mode, test_type, reset):
    vc_transformed = {cast_graph_name_to_triple(k): v for k, v in vc.items()}

    vc_transformed = {
        "->".join([f"{x}" for x in k]) if isinstance(k, tuple) else k: v
        for k, v in vc_transformed.items()
    }

    if reset:
        FileHandle.dump(
            vc_transformed,
            join(current_path, f"./ref/{test_type}/{mode}_sizes.yaml"),
        )

    else:
        ref_vc = FileHandle.load(f"test.ref.{test_type}", f"{mode}_sizes.yaml")
        if not equals(vc_transformed, ref_vc):
            print(f" mode: {mode}")
            for k, v in ref_vc.items():
                print(
                    f" {k} expected: {v}, received:"
                    f" {vc_transformed[k] if k in vc_transformed else None}"
                )
        assert equals(vc_transformed, ref_vc)
