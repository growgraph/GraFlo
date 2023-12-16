import logging

from suthing import FileHandle

from graph_cast.architecture import TConfigurator
from graph_cast.architecture.ptree import ParsingTree
from graph_cast.architecture.schema import VertexConfig

logger = logging.getLogger(__name__)


def test_parsing_tree():
    config = FileHandle.load(f"test.config.schema", f"kg_v3b.yaml")
    vertex_config = VertexConfig.from_dict(config["vertex_collections"])
    pt = ParsingTree(config["json"], vertex_config=vertex_config)
    assert len(pt.root.children) == 5


# def test_table_transform_collection_map():
#     config = FileHandle.load(f"test.config.schema", "ibes.yaml")
#     conf_obj = TConfigurator(config)
#     conf_obj.set_mode("ibes")
#     assert conf_obj.table_config["ibes"].fields() == {
#         "erec",
#         "initial",
#         "oftic",
#         "etext",
#         "aname",
#         "cname",
#         "cusip",
#         "itext",
#         "datetime_announce",
#         "irec",
#         "datetime_review",
#         "last_name",
#     }
