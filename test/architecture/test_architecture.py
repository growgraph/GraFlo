import logging
import sys
import unittest
from os.path import dirname, realpath

from graph_cast.architecture import JConfigurator
from graph_cast.architecture.ptree import ParsingTree
from graph_cast.architecture.schema import VertexConfig
from graph_cast.util import ResourceHandler

logger = logging.getLogger(__name__)


class TestJConfig(unittest.TestCase):
    cpath = dirname(realpath(__file__))

    @unittest.skip("")
    def test_edges_upsert(self):
        config = ResourceHandler.load(f"conf.json", f"kg_v1.yaml")
        conf_obj = JConfigurator(config)
        self.assertTrue(
            conf_obj.graph_config._edges[
                ("mention", "mention")
            ].weight_vertices,
            ["publication"],
        )

    def test_parsing_tree(self):
        config = ResourceHandler.load(f"conf.json", f"kg_v3.yaml")
        vertex_config = VertexConfig(config["vertex_collections"])
        pt = ParsingTree(config["json"], vertex_config=vertex_config)
        print(pt)


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    unittest.main()
