import logging
import unittest

from graph_cast.architecture.general import Mapper

logger = logging.getLogger(__name__)


class TestMapper(unittest.TestCase):
    def test_transform(self):
        kwargs = {"map": {"uid": "_key"}}
        m = Mapper(**kwargs)
        self.assertTrue(m({"uid": 123}) == {"_key": 123})

    def test_transform_fname(self):
        kwargs = {"map": {"_filename": "ticker"}, "filename": "AAPL"}
        m = Mapper(**kwargs)
        self.assertTrue(m({"uid": 123}) == {"ticker": "AAPL"})

    def test_transform_fname_dyn(self):
        kwargs = {"map": {"_filename": "ticker"}}
        m = Mapper(**kwargs)
        m.update(**{"filename": "AAPL"})
        self.assertTrue(m({"uid": 123}) == {"ticker": "AAPL"})

    def test_transform_name(self):
        kwargs = {"map": {"Close": {"value": "value", "key": "name"}}}
        doc = {"key_a": "value_a", "Close": 15.35, "key_b": "value_b"}
        m = Mapper(**kwargs)
        r = m(doc)
        self.assertTrue(r == {"value": 15.35, "name": "Close"})


if __name__ == "__main__":
    unittest.main()
