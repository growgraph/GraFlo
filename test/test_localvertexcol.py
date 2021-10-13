import unittest
import logging
from graph_cast.architecture.general import LocalVertexCollections

logger = logging.getLogger(__name__)


class TestLVC(unittest.TestCase):
    def test_init(self):
        kwargs = [
            {"type": "a", "map": {"uid": "_key"}},
            {"type": "a", "map": {"lala": "_key"}},
            {"type": "v", "map": {"blah": "oops"}},
        ]
        m = LocalVertexCollections(kwargs)
        self.assertTrue(len(m._vcollections["a"]) == 2)

    def test_iterate(self):
        kwargs = [
            {"type": "a", "map": {"uid": "_key"}},
            {"type": "a", "map": {"lala": "_key"}},
            {"type": "v", "map": {"blah": "oops"}},
        ]
        m = LocalVertexCollections(kwargs)
        iterated = [item for item in m]
        self.assertTrue(len(iterated) == 3)


if __name__ == "__main__":
    unittest.main()
