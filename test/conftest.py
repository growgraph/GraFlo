import io
import logging
from os.path import dirname, join, realpath
from pathlib import Path

import pandas as pd
import pytest
import yaml
from suthing import FileHandle, equals

from graphcast.architecture.schema import Schema
from graphcast.architecture.util import cast_graph_name_to_triple
from graphcast.caster import Caster
from graphcast.util.misc import sorted_dicts

logger = logging.getLogger(__name__)


def pytest_addoption(parser):
    parser.addoption("--reset", action="store_true")


@pytest.fixture(scope="session", autouse=True)
def reset(request):
    return request.config.getoption("reset")


@pytest.fixture(scope="function")
def current_path():
    return dirname(realpath(__file__))


def fetch_schema_dict(mode):
    schema_dict = FileHandle.load("test.config.schema", f"{mode}.yaml")
    return schema_dict


def fetch_schema_obj(mode):
    schema_dict = fetch_schema_dict(mode)
    schema_obj = Schema.from_dict(schema_dict)
    return schema_obj


@pytest.fixture(scope="function")
def schema():
    return fetch_schema_dict


@pytest.fixture(scope="function")
def schema_obj():
    return fetch_schema_obj


def ingest_atomic(conn_conf, current_path, test_db_name, mode, n_cores=1):
    schema_o = fetch_schema_obj(mode)
    path = Path(current_path) / f"data/{mode}"

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
        _ = sample.pop("_fishbowl", None)
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
        sample_ref = FileHandle.load(f"test.ref.{test_type}", f"{mode}_{kind}.{ext}")
        flag = equals(sample_transformed, sample_ref)
        if not flag:
            logger.error(f" mode: {mode}")
            if isinstance(sample_ref, dict):
                for k, v in sample_ref.items():
                    if k not in sample_transformed or v != sample_transformed[k]:
                        logger.error(
                            f"for {k} expected: {v} received: {sample_transformed[k] if k in sample_transformed else None}"
                        )

            elif isinstance(sample_ref, list):
                for j, (x, y) in enumerate(zip(sample_ref, sample_transformed)):
                    if x != y:
                        logger.error(f"for item {j}\nexpected: {x}\nreceived: {y}")

        assert flag


@pytest.fixture()
def df_ibes() -> pd.DataFrame:
    df0_str = """TICKER,CUSIP,CNAME,OFTIC,ACTDATS,ESTIMID,ANALYST,ERECCD,ETEXT,IRECCD,ITEXT,EMASKCD,AMASKCD,USFIRM,ACTTIMS,REVDATS,REVTIMS,ANNDATS,ANNTIMS
0000,87482X10,TALMER BANCORP,TLMR,20140310,RBCDOMIN,ARFSTROM      J,2,OUTPERFORM,2,BUY,00000659,00071182,1,8:54:03,20160126,9:35:52,20140310,0:20:00
0000,87482X10,TALMER BANCORP,TLMR,20140311,JPMORGAN,ALEXOPOULOS   S,,OVERWEIGHT,2,BUY,00001243,00079092,1,17:10:47,20160126,10:09:34,20140310,0:25:00"""
    return pd.read_csv(
        io.StringIO(df0_str),
        sep=",",
        dtype={"TICKER": str, "ANNDATS": str, "REVDATS": str},
    )


@pytest.fixture()
def df_ticker() -> pd.DataFrame:
    df0_str = """Date,Open,High,Low,Close,Volume,Dividends,Stock Splits,__ticker
2014-04-15,17.899999618530273,17.920000076293945,15.149999618530273,15.350000381469727,3531700,0,0,AAPL
2014-04-16,15.350000381469727,16.09000015258789,15.210000038146973,15.619999885559082,266500,0,0,AAPL"""
    return pd.read_csv(
        io.StringIO(df0_str),
        sep=",",
    )


@pytest.fixture()
def df_transform_collision() -> pd.DataFrame:
    df0_str = """id,name,pet_name
A0,Joe,Rex"""
    return pd.read_csv(
        io.StringIO(df0_str),
        sep=",",
    )


@pytest.fixture()
def row_doc_ibes() -> dict[str, list]:
    return {
        "agency": [{"aname": "RBCDOMIN"}],
        "analyst": [{"initial": "J", "last_name": "ARFSTROM"}],
        "publication": [
            {"datetime_announce": "2014-03-10T0:20:00Z"},
            {"datetime_review": "2016-01-26T9:35:52Z"},
        ],
        "recommendation": [
            {"erec": 2.0, "etext": "OUTPERFORM", "irec": 2, "itext": "BUY"}
        ],
        "ticker": [{"cname": "TALMER BANCORP", "cusip": "87482X10", "oftic": "TLMR"}],
    }


@pytest.fixture()
def row_resource_transform_collision():
    tc = yaml.safe_load(
        """
        name: pets
        transforms:
        -   image: pet
            map:
                pet_name: name
    """
    )
    return tc


@pytest.fixture()
def vertex_config_transform_collision():
    vc = yaml.safe_load(
        """
        vertices:
        -
            name: person
            dbname: people
            fields:
            -   id
            -   name
        -
            name: pet
            dbname: pets
            fields:
            -   name
    """
    )
    return vc


@pytest.fixture()
def resource_with_dynamic_relations():
    vc = yaml.safe_load(
        """
    general:
        name: openalex
    resources:
        tree_likes:
           -   name: institutions
            root:
                children:
                -   type: vertex
                    name: institution
                    transforms:
                    -   name: keep_suffix_id
                    -   name: keep_suffix_id
                        fields:
                        -   ror
                -   key: associated_institutions
                    children:
                    -   type: vertex
                        name: institution
                        transforms:
                        -   name: keep_suffix_id
                    -   type: edge
                        edge:
                            source: institution
                            target: institution
    #                        relation:
    vertex_config:
    vertices:
    -   name: institution
        dbname: institutions
        fields:
        -   _key
        -   display_name
        -   country
        -   type
        -   ror
        -   grid
        -   wikidata
        -   mag
        -   created_date
        -   updated_date
        indexes:
        -   fields:
            -   _key
        -   unique: false
            type: fulltext
            fields:
            -   display_name
        -   unique: false
            fields:
            -   type
    edge_config:
        edges: []
    transforms:
        keep_suffix_id:
            foo: split_keep_part
            module: graphcast.util.transform
            params:
                sep: "/"
                keep: -1
            input:
            -   id
            output:
            -   _key

    """
    )
    return vc


@pytest.fixture()
def resource_openalex_works():
    an = yaml.safe_load("""
    -   vertex: work
        discriminant: _top_level
    -   name: keep_suffix_id
        foo: split_keep_part
        module: graphcast.util.transform
        params:
            sep: "/"
            keep: -1
        input:
        -   id
        output:
        -   _key
    -   name: keep_suffix_id
        params:
            sep: "/"
            keep: [-2, -1]
        input:
        -   doi
        output:
        -   doi
    -   key: referenced_works
        apply:
        -   vertex: work
        -   name: keep_suffix_id
    -   source: work
        target: work
        source_discriminant: _top_level
    """)
    return an
