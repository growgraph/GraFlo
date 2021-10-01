from graph_cast.architecture.schema import VertexConfig, GraphConfig
from copy import deepcopy


class JConfigurator:
    weights_definition = {}

    def __init__(self, config):
        self.vertex_config = VertexConfig(config["vertex_collections"])
        self.graph_config = GraphConfig(
            config["edge_collections"], self.vertex_config.name
        )
        self.json = deepcopy(config["json"])

    def graph(self, u, v):
        return self.graph_config.graph(u, v)


