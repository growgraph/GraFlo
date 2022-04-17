import unittest
import logging
import argparse
from graph_cast.input.csv import table_to_vcollections
from graph_cast.util import ResourceHandler, equals
from graph_cast.architecture.table import TConfigurator
from graph_cast.util.transform import pick_unique_dict


logger = logging.getLogger(__name__)


class TestIngestCSV(unittest.TestCase):
    modes = ["ibes", "ticker"]

    def __init__(self, reset):
        super().__init__()
        self.reset = reset

    def _atomic(self, mode):
        df = ResourceHandler.load(f"test.data.all", f"{mode}.csv.gz")
        config = ResourceHandler.load(f"conf", f"{mode}.yaml")
        conf_obj = TConfigurator(config)

        header = df.columns
        header_dict = dict(zip(header, range(len(header))))
        lines = list(df.values)
        conf_obj.set_mode(mode)
        vdocuments, edocuments = table_to_vcollections(
            lines,
            header_dict,
            conf_obj,
        )

        vc = {k: len(pick_unique_dict(v)) for k, v in vdocuments.items()}

        if not self.reset:
            ref_vc = ResourceHandler.load(f"test.ref", f"{mode}_sizes.yaml")
            flag = equals(vc, ref_vc)
            if not flag:
                print(vc)
                print(ref_vc)
            self.assertTrue(flag)
        else:
            ResourceHandler.dump(vc, f"./test/ref/{mode}_sizes.yaml")

    def runTest(self):
        for mode in self.modes:
            self._atomic(mode)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true", help="reset test results")
    args = parser.parse_args()
    suite = unittest.TestSuite()
    suite.addTest(TestIngestCSV(args.reset))
    unittest.TextTestRunner(verbosity=2).run(suite)
