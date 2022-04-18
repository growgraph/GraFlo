import unittest
import logging
import argparse
from os.path import join, dirname, realpath
from graph_cast.input import jsondoc_to_collections
from graph_cast.util import ResourceHandler, equals
from graph_cast.architecture import JConfigurator

logger = logging.getLogger(__name__)


class TestTransformJsonlike(unittest.TestCase):
    cpath = dirname(realpath(__file__))
    modes = ["freshcaller"]

    def __init__(self, reset):
        super().__init__()
        self.reset = reset

    def _atomic(self, mode):
        jsonlike = ResourceHandler.load(f"test.data.json.{mode}", f"{mode}.json.gz")
        config = ResourceHandler.load(f"conf.json", f"{mode}.yaml")
        conf_obj = JConfigurator(config)

        defdict = jsondoc_to_collections(jsonlike[0], conf_obj)

        vc = {k: len(v) for k, v in defdict.items()}

        if not self.reset:
            ref_vc = ResourceHandler.load(f"test.ref.json", f"{mode}_sizes.yaml")
            flag = equals(vc, ref_vc)
            if not flag:
                print(vc)
                print(ref_vc)
            self.assertTrue(flag)
        else:
            ResourceHandler.dump(vc, join(self.cpath, f"../ref/json/{mode}_sizes.yaml"))

    def runTest(self):
        for mode in self.modes:
            self._atomic(mode)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true", help="reset test results")
    args = parser.parse_args()
    suite = unittest.TestSuite()
    suite.addTest(TestTransformJsonlike(args.reset))
    unittest.TextTestRunner(verbosity=2).run(suite)
