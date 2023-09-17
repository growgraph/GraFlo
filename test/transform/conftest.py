import io
from os.path import dirname, join, realpath

import pandas as pd
import pytest
import yaml
from suthing import equals

from graph_cast.architecture import JConfigurator, TConfigurator
from graph_cast.input import jsondoc_to_collections, table_to_collections
from graph_cast.input.util import list_to_dict_edges, list_to_dict_vertex
from graph_cast.main import ingest_files
from graph_cast.onto import InputType, InputTypeFileExtensions
from graph_cast.util import ResourceHandler
from graph_cast.util.transform import pick_unique_dict


@pytest.fixture()
def current_path():
    return dirname(realpath(__file__))


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

        docs = table_to_collections(
            lines,
            header_dict,
            conf_obj,
        )

        vdocuments = list_to_dict_vertex(docs)
        edocuments = list_to_dict_edges(docs)

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


@pytest.fixture()
def table_config_ibes():
    tc = yaml.safe_load("""
        tabletype: ibes
        encoding: ISO-8859-1
        transforms:
        -   foo: parse_date_ibes
            module: graph_cast.util.transform
            input:
            -   ANNDATS
            -   ANNTIMS
            output:
            -   datetime_announce
        -   foo: parse_date_ibes
            module: graph_cast.util.transform
            input:
            -   REVDATS
            -   REVTIMS
            output:
            -   datetime_review
        -   foo: cast_ibes_analyst
            module: graph_cast.util.transform
            input:
            -   ANALYST
            output:
            -   last_name
            -   initial
        -   map:
                CUSIP: cusip
                CNAME: cname
                OFTIC: oftic
        -   map:
                ESTIMID: aname
        -   map:
                ERECCD: erec
                ETEXT: etext
                IRECCD: irec
                ITEXT: itext
    """)
    return tc


@pytest.fixture()
def vertex_config_ibes():
    vc = yaml.safe_load("""
        blanks:
        -   publication
        collections:
            publication:
                basename: publications
                fields:
                -   datetime_review
                -   datetime_announce
                extra_index:
                -   type: hash
                    unique: false
                    fields:
                    -   datetime_review
                -   type: hash
                    unique: false
                    fields:
                    -   datetime_announce
            ticker:
                basename: tickers
                fields:
                -   cusip
                -   cname
                -   oftic
                index:
                -   cusip
                -   cname
                -   oftic
            agency:
                basename: agencies
                fields:
                -   aname
                index:
                -   aname
            analyst:
                basename: analysts
                fields:
                -   last_name
                -   initial
                index:
                -   last_name
                -   initial
            recommendation:
                basename: recommendations
                fields:
                -   erec
                -   etext
                -   irec
                -   itext
                index:
                -   irec
    """)
    return vc


@pytest.fixture()
def edge_config_ibes():
    vc = yaml.safe_load("""
        main:
        -   source: publication
            target: ticker
        -   source: analyst
            target: agency
            weight:
            -   datetime_review
            -   datetime_announce
        -   source: analyst
            target: publication
        -   source: publication
            target: recommendation
    """)
    return vc


@pytest.fixture()
def tconf_ibes(vertex_config_ibes, edge_config_ibes, table_config_ibes):
    config = {
        "general": {"name": "ibes"},
        "table": [table_config_ibes],
        "vertex_collections": vertex_config_ibes,
        "edge_collections": edge_config_ibes,
    }
    tc = TConfigurator(config)
    return tc


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
def vertex_config_transform_collision():
    vc = yaml.safe_load("""
        collections:
            person:
                basename: people
                fields:
                -   id
                -   name
            pet:
                basename: pets
                fields:
                -   name
    """)
    return vc


@pytest.fixture()
def table_config_transform_collision():
    tc = yaml.safe_load("""
        tabletype: pets
        transforms:
        -   image: pet
            map:
                pet_name: name
    """)
    return tc


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
        "ticker": [
            {"cname": "TALMER BANCORP", "cusip": "87482X10", "oftic": "TLMR"}
        ],
    }
