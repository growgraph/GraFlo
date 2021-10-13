import unittest
import logging
from os.path import join, dirname, realpath
import yaml
import argparse
from pprint import pprint
from graph_cast.architecture.schema import Filter, Condition
from graph_cast.architecture.table import TConfigurator
from graph_cast.util.io import Chunker
from graph_cast.input.csv_abs import table_to_vcollections

logger = logging.getLogger(__name__)


class TestFilter(unittest.TestCase):
    case_a = {
        "a": {"field": "name", "foo": "__eq__", "value": "Open"},
        "b": {"field": "value", "foo": "__gt__", "value": 0},
    }

    cpath = dirname(realpath(__file__))

    set_reference = False

    # def __init__(self, set_ref=False):
    #     super().__init__()
    # self.set_reference = set_ref

    def test_condition(self):
        m = Condition(**self.case_a["a"])
        doc = {"name": "Open"}
        self.assertTrue(m(**doc))

    def test_filter(self):
        m = Filter(**self.case_a)

        doc = {"name": "Open", "value": -1}
        self.assertFalse(m(doc))

        doc = {"name": "Open", "value": 5.0}
        self.assertTrue(m(doc))

        doc = {"name": "Close", "value": -1.0}
        self.assertTrue(m(doc))

    def test_transformproblems(self):
        mode = "ticker"
        if not self.set_reference:
            ref_path = join(self.cpath, f"./ref/{mode}_sizes.yaml")
            with open(ref_path, "r") as f:
                ref_sizes = yaml.load(f, Loader=yaml.FullLoader)

        tabular_resource = join(self.cpath, f"./data/all/ticker.use_filter.csv.gz")

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
        test_sizes = {
            "vdocs": {
                vcol: [len(x) for x in item] for vcol, item in vdocuments.items()
            },
            "edocs": {vcol: len(item) for vcol, item in edocuments.items()},
        }

        if self.set_reference:
            ref_path = join(self.cpath, f"./ref/{mode}_sizes.yaml")
            with open(ref_path, "w") as file:
                yaml.dump(test_sizes, file)
        else:
            print("\n")
            for k in test_sizes:
                test_item = test_sizes[k]
                ref_item = ref_sizes[k]
                for q in test_item:
                    pprint(f"{k} {q} {test_item[q]} {ref_item[q]}")

            self.assertTrue(test_sizes == ref_sizes)


if __name__ == "__main__":
    unittest.main()
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--reset", action="store_true", help="reset test results")
    # args = parser.parse_args()
    # suite = unittest.TestSuite()
    # suite.addTest(TestFilter(args.reset))
    # unittest.TextTestRunner(verbosity=2).run(suite)
