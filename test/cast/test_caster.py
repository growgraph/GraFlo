import logging
from os.path import dirname, join, realpath

import pytest
from suthing import FileHandle, equals

from graph_cast.caster import Caster
from graph_cast.onto import InputTypeFileExtensions

logger = logging.getLogger(__name__)


@pytest.fixture()
def current_path():
    return dirname(realpath(__file__))


@pytest.fixture(scope="function")
def modes():
    return ["kg_v3b", "ibes"]


def cast_it(schema_obj, data):
    caster = Caster(schema_obj)
    docs = caster.cast(data)
    return docs


def test_transform(modes, schema_obj, current_path, reset):
    for mode in modes:
        schema_obj = schema_obj(mode)
        rr = schema_obj.fetch_resource()
        data_obj = FileHandle.load(
            f"test.data.{InputTypeFileExtensions[rr.resource_type][0]}.{mode}",
            f"{mode}.{InputTypeFileExtensions[rr.resource_type][0]}.gz",
        )

        g = cast_it(schema_obj, data_obj)

        vc = {k: len(v) for k, v in g[0].items()}
        verify(vc, current_path=current_path, mode=mode, reset=reset)


def verify(vc, current_path, mode, reset):
    vc_tranformed = {
        "->".join([f"{x}" for x in k]) if isinstance(k, tuple) else k: v
        for k, v in vc.items()
    }

    if reset:
        FileHandle.dump(
            vc_tranformed,
            join(current_path, f"../ref/transform/{mode}_sizes.yaml"),
        )

    else:
        ref_vc = FileHandle.load(f"test.ref.transform", f"{mode}_sizes.yaml")
        if not equals(vc, ref_vc):
            print(f" mode: {mode}")
            for k, v in ref_vc.items():
                print(
                    f" {k} expected: {v}, received:"
                    f" {vc[k] if k in vc else None}"
                )
        assert equals(vc_tranformed, ref_vc)
