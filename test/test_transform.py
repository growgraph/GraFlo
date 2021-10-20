import unittest
from os.path import join, dirname, realpath
import logging
from graph_cast.architecture.general import Transform
import yaml
from graph_cast.architecture.table import TConfigurator
from graph_cast.util.io import Chunker
from graph_cast.input.csv_abs import table_to_vcollections


logger = logging.getLogger(__name__)


class TestTransform(unittest.TestCase):
    cpath = dirname(realpath(__file__))

    def test_transform_int(self):
        kwargs = {"module": "builtins", "foo": "int", "input": "x", "output": "y"}
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
        tabular_resource = join(self.cpath, f"./data/all/ticker.use_tranform.csv.gz")

        config_path = join(self.cpath, f"../conf/{mode}.yaml")

        with open(config_path, "r") as f:
            config = yaml.load(f, Loader=yaml.FullLoader)

        conf = TConfigurator(config)
        conf.set_mode("_all")

        chk = Chunker(tabular_resource, batch_size=10000000, encoding=conf.encoding)
        conf.set_current_resource_name(tabular_resource)
        header = chk.pop_header()
        header_dict = dict(zip(header, range(len(header))))

        while not chk.done:
            lines = chk.pop()
            if lines:
                vdocuments, edocuments = table_to_vcollections(
                    lines,
                    header_dict,
                    conf,
                )
            pass


if __name__ == "__main__":
    unittest.main()
