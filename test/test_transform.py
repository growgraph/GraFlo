import unittest
import logging
from graph_cast.architecture.table import Transform
logger = logging.getLogger(__name__)


class TestIngestCSV(unittest.TestCase):
    def test_transform(self):
        kwargs = {
            "module": "builtins",
            "foo": "round",
            "input": "x",
            "output": "y",
            "params":
                {
                    "ndigits": 3}
        }
        t = Transform(**kwargs)
        print(t(0.1234))


if __name__ == "__main__":
    unittest.main()
