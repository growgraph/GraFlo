import argparse
import logging
import unittest
from os.path import dirname, realpath

from graph_cast.architecture.schema import Condition, Filter

logger = logging.getLogger(__name__)


class TestFilter(unittest.TestCase):
    case_a = {
        "a": {"field": "name", "foo": "__eq__", "value": "Open"},
        "b": {"field": "value", "foo": "__gt__", "value": 0},
    }

    cpath = dirname(realpath(__file__))

    def __init__(self, reset):
        super().__init__()
        self.reset = reset

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

    def runTest(self):
        self.test_filter()
        self.test_condition()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--reset", action="store_true", help="reset test results"
    )
    args = parser.parse_args()
    suite = unittest.TestSuite()
    unittest.TextTestRunner(verbosity=2).run(suite)
