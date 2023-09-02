from os.path import dirname, join, realpath

import pytest
from suthing import FileHandle, equals

from graph_cast.architecture import JConfigurator, TConfigurator
from graph_cast.input import jsondoc_to_collections, table_to_collections
from graph_cast.main import ingest_files
from graph_cast.onto import InputType, InputTypeFileExtensions
from graph_cast.util import ResourceHandler
from graph_cast.util.transform import pick_unique_dict


@pytest.fixture()
def current_path():
    return dirname(realpath(__file__))


def ingest_atomic(conn_conf, current_path, test_db_name, input_type, mode):
    path = join(current_path, f"../data/{input_type}/{mode}")
    schema = ResourceHandler.load(f"conf.{input_type}", f"{mode}.yaml")

    conn_conf.database = test_db_name
    ingest_files(
        fpath=path,
        schema=schema,
        conn_conf=conn_conf,
        input_type=input_type,
        limit_files=None,
        clean_start=True,
    )


def transform_it(current_path, input_type, mode, reset):
    data_obj = ResourceHandler.load(
        f"test.data.{input_type}.{mode}",
        f"{mode}.{InputTypeFileExtensions[input_type][0]}.gz",
    )
    config = ResourceHandler.load(f"conf.{input_type}", f"{mode}.yaml")

    if input_type == InputType.TABLE:
        conf_obj = TConfigurator(config)

        header = data_obj.columns
        header_dict = dict(zip(header, range(len(header))))
        lines = list(data_obj.values)
        conf_obj.set_mode(mode)

        vdocuments, edocuments = table_to_collections(
            lines,
            header_dict,
            conf_obj,
        )

        vc = {k: len(pick_unique_dict(v)) for k, v in vdocuments.items()}

    elif input_type == InputType.JSON:
        conf_obj = JConfigurator(config)

        defdict = jsondoc_to_collections(data_obj[0], conf_obj)

        vc = {k: len(v) for k, v in defdict.items()}
    else:
        raise ValueError(f"Unknown {input_type}")

    verify(vc, current_path, mode, reset)


def verify(vc, current_path, mode, reset):
    vc_tranformed = {
        "->".join(list(k)) if isinstance(k, tuple) else k: v
        for k, v in vc.items()
    }

    if reset:
        ResourceHandler.dump(
            vc_tranformed,
            join(current_path, f"../ref/transform/{mode}_sizes.yaml"),
        )

    else:
        ref_vc = ResourceHandler.load(
            f"test.ref.transform", f"{mode}_sizes.yaml"
        )
        assert equals(vc_tranformed, ref_vc)
