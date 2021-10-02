from graph_cast.architecture.schema import VertexConfig, GraphConfig


class Configurator:
    def __init__(self, config):
        self.vertex_config = VertexConfig(config["vertex_collections"])
        self.graph_config = GraphConfig(
            config["edge_collections"],
            self.vertex_config.name,
            config["json"] if "json" in config else None,
        )
