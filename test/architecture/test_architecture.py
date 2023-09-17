import logging

from suthing import FileHandle

from graph_cast.architecture import JConfigurator, TConfigurator
from graph_cast.architecture.ptree import ParsingTree
from graph_cast.architecture.schema import VertexConfig

logger = logging.getLogger(__name__)


# def test_parsing_tree():
#     config = FileHandle.load(f"conf.json", f"kg_v3.yaml")
#     vertex_config = VertexConfig(config["vertex_collections"])
#     pt = ParsingTree(config["json"], vertex_config=vertex_config)
#
#
# def test_table_transform_collection_map():
#     config = FileHandle.load(f"conf.table", "ibes.yaml")
#     conf_obj = TConfigurator(config)
#     conf_obj.set_mode("ibes")
#     pass


def test_table_transform_collection_map():
    config = FileHandle.load(f"test.schema", "ibes.yaml")
    conf_obj = TConfigurator(config)
    conf_obj.set_mode("ibes")
    pass
