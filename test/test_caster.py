import logging
import os
import pathlib
from os.path import dirname, realpath
from test.conftest import fetch_schema_obj, verify

import pytest
from suthing import FileHandle

from graph_cast.caster import Caster
from graph_cast.onto import InputTypeFileExtensions

logger = logging.getLogger(__name__)


@pytest.fixture()
def current_path():
    return dirname(realpath(__file__))


@pytest.fixture(scope="function")
def modes():
    return ["kg_v3b", "ibes"]


def cast(modes, schema_obj, current_path, level, reset, n_threads=1):
    for mode in modes:
        # work with main resource
        resource_name = mode.split("_")[0]
        schema = schema_obj(mode)

        caster = Caster(schema, n_threads=n_threads)
        rr = schema.fetch_resource(name=resource_name)
        fname = os.path.join(
            current_path,
            f"./data/{InputTypeFileExtensions[rr.resource_type][0]}/{mode}",
            f"{mode}.{InputTypeFileExtensions[rr.resource_type][0]}.gz",
        )

        data_obj = FileHandle.load(
            f"test.data.{InputTypeFileExtensions[rr.resource_type][0]}.{mode}",
            f"{mode}.{InputTypeFileExtensions[rr.resource_type][0]}.gz",
        )

        if level == 0:
            caster.process_resource(
                resource=pathlib.Path(fname), resource_name=resource_name
            )
        elif level == 1:
            caster.process_resource(resource=data_obj, resource_name=resource_name)
        elif level == 2:
            data_obj = FileHandle.load(
                f"test.data.{InputTypeFileExtensions[rr.resource_type][0]}.{mode}",
                f"{mode}.{InputTypeFileExtensions[rr.resource_type][0]}.gz",
            )

            data = caster.normalize_resource(data_obj)
            graph = caster.cast_normal_resource(data, resource_name=resource_name)

            graph.pick_unique()

            vc = {k: len(v) for k, v in graph.items()}
            verify(
                vc,
                current_path=current_path,
                mode=mode,
                test_type="transform",
                reset=reset,
            )


def test_cast(modes, schema_obj, current_path, reset):
    cast(modes, schema_obj, current_path, level=0, reset=reset)
    cast(modes, schema_obj, current_path, level=1, reset=reset)
    cast(modes, schema_obj, current_path, level=2, reset=reset)
    cast(modes, schema_obj, current_path, level=2, reset=reset, n_threads=4)


def test_edge_index(batch_edge_index):
    assert len(batch_edge_index) == 5
    cschema = fetch_schema_obj("kg.edge.index")
    caster = Caster(cschema, n_threads=1, dry=False)

    graph = caster.cast_normal_resource(batch_edge_index, resource_name="metrics_load")
    assert len(graph.edges[("publication", "metric", None)]) == 5
