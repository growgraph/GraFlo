import logging
import unittest
from os.path import dirname, realpath

import yaml

from graph_cast.architecture.table import TConfigurator
from graph_cast.architecture.transform import Transform
from graph_cast.input import table_to_collections
from graph_cast.util import ResourceHandler, equals
from graph_cast.util.io import Chunker

logger = logging.getLogger(__name__)


class TestTransform(unittest.TestCase):
    cpath = dirname(realpath(__file__))

    def test_transform_int(self):
        kwargs = {
            "module": "builtins",
            "foo": "int",
            "input": "x",
            "output": "y",
        }
        t = Transform(**kwargs)
        self.assertTrue(t("12345") == 12345)

    def test_transform(self):
        kwargs = {
            "module": "builtins",
            "foo": "round",
            "input": "x",
            "output": "y",
            "params": {"ndigits": 3},
        }
        t = Transform(**kwargs)
        self.assertTrue(t(0.1234) == 0.123)

    def test_transform_problems(self):
        mode = "ticker"
        config = ResourceHandler.load(f"conf.table", f"{mode}.yaml")
        conf = TConfigurator(config)

        conf.set_mode("_all")

        chk = Chunker(
            fname=None,
            pkg_spec=(f"test.data.all", f"{mode}.use_tranform.csv.gz"),
            batch_size=10000000,
            encoding=conf.encoding,
        )
        # conf.set_current_resource_name(tabular_resource)
        header = chk.pop_header()
        header_dict = dict(zip(header, range(len(header))))

        while not chk._done:
            lines = chk.pop()
            if lines:
                vdocuments, edocuments = table_to_collections(
                    lines,
                    header_dict,
                    conf,
                )


if __name__ == "__main__":
    unittest.main()
