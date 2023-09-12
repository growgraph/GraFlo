import logging

import pytest

from graph_cast.architecture.table import TConfigurator
from graph_cast.architecture.transform import Transform
from graph_cast.input import table_to_collections
from graph_cast.util import ResourceHandler, equals
from graph_cast.util.io import Chunker
from graph_cast.util.transform import parse_multi_item

logger = logging.getLogger(__name__)


@pytest.fixture
def quoted_multi_row():
    return """1486058874058,"['id:206158957580, name:Marcello Martini'\n'id:360777873683, name:F. Giudicepietro'\n"id:489626818966, name:Luca D'Auria"]",[127313418 165205528],2015,10.1038/SREP13100"""


@pytest.fixture
def quoted_multi_item():
    return """['id:206158957580, name:Marcello Martini'
 'id:360777873683, name:F. Giudicepietro'
 "id:489626818966, name:Luca D'Auria"]"""


def test_to_int():
    kwargs = {
        "module": "builtins",
        "foo": "int",
        "input": "x",
        "output": "y",
    }
    t = Transform(**kwargs)
    assert t("12345") == 12345


def test_round():
    kwargs = {
        "module": "builtins",
        "foo": "round",
        "input": "x",
        "output": "y",
        "params": {"ndigits": 3},
    }
    t = Transform(**kwargs)
    assert t(0.1234) == 0.123


def test_parse_multi_item(quoted_multi_item):
    r = parse_multi_item(
        quoted_multi_item, mapper={"name": "full_name"}, direct=["id"]
    )
    assert r[0]["full_name"] == "Luca D'Auria"
    assert r[-1]["id"] == "360777873683"


# def test_transform_problems():
#     mode = "ticker"
#     config = ResourceHandler.load(f"conf.table", f"{mode}.yaml")
#     conf = TConfigurator(config)
#
#     conf.set_mode("_all")
#
#     chk = Chunker(
#         fname=None,
#         pkg_spec=(f"test.data.all", f"{mode}.use_tranform.csv.gz"),
#         batch_size=10000000,
#         encoding=conf.encoding,
#     )
#     # conf.set_current_resource_name(tabular_resource)
#     header = chk.pop_header()
#     header_dict = dict(zip(header, range(len(header))))
#
#     while not chk._done:
#         lines = chk.pop()
#         if lines:
#             vdocuments, edocuments = table_to_collections(
#                 lines,
#                 header_dict,
#                 conf,
#             )
#
#
