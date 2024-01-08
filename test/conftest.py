from os.path import dirname, join, realpath
from pathlib import Path

import pytest
from suthing import FileHandle, equals

from graph_cast.architecture.onto import cast_graph_name_to_triple
from graph_cast.architecture.schema import Schema
from graph_cast.caster import Caster
from graph_cast.onto import InputTypeFileExtensions
from graph_cast.util.misc import sorted_dicts


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


def ingest_atomic(conn_conf, current_path, test_db_name, mode, n_cores=1):
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
        n_cores=n_cores,
        path=path,
        limit_files=None,
        clean_start=True,
        conn_conf=conn_conf,
    )


def verify(sample, current_path, mode, test_type, kind="sizes", reset=False):
    ext = "yaml"
    if kind == "sizes":
        sample_transformed = {
            cast_graph_name_to_triple(k): v for k, v in sample.items()
        }

        sample_transformed = {
            "->".join([f"{x}" for x in k]) if isinstance(k, tuple) else k: v
            for k, v in sample_transformed.items()
        }
    elif kind == "indexes":
        sample_transformed = sorted_dicts(sample)
    elif kind == "contents":
        sample_transformed = sorted_dicts(sample)
    else:
        raise ValueError(f"value {kind} not accepted")

    if reset:
        FileHandle.dump(
            sample_transformed,
            join(current_path, f"./ref/{test_type}/{mode}_{kind}.{ext}"),
        )

    else:
        sample_ref = FileHandle.load(
            f"test.ref.{test_type}", f"{mode}_{kind}.{ext}"
        )
        flag = equals(sample_transformed, sample_ref)
        if not flag:
            print(f" mode: {mode}")
            if isinstance(sample_ref, dict):
                for k, v in sample_ref.items():
                    if v != sample_transformed[k]:
                        print(
                            f"for {k}\n"
                            f"expected: {v}\n"
                            "received:"
                            f" {sample_transformed[k] if k in sample_transformed else None}"
                        )
            elif isinstance(sample_ref, list):
                for j, (x, y) in enumerate(
                    zip(sample_ref, sample_transformed)
                ):
                    if x != y:
                        print(f"for item {j}\nexpected: {x}\nreceived: {y}")

        assert flag
