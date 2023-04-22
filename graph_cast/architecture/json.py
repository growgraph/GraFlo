from copy import deepcopy

from graph_cast.architecture.general import Configurator
from graph_cast.architecture.ptree import ParsingTree
from graph_cast.architecture.schema import Edge


class JConfigurator(Configurator):
    def __init__(self, config):
        super().__init__(config)

        self.json = deepcopy(config["json"])
        # which collections should be merged? (if they are found in different parts of json doc)

        self.tree = ParsingTree(
            config["json"], vertex_config=self.vertex_config
        )

        self.graph_config.parse_edges(self.tree)

        self.merge_collections = tuple()

        self.post_weights: list[Edge] = []

        if "extra" in config:
            config_extra = config["extra"]
            if "merge_collections" in config_extra:
                self.merge_collections = tuple(
                    config_extra["merge_collections"]
                )
            if "weights" in config_extra:
                for item in config_extra["weights"]:
                    self.post_weights = [Edge(item, vconf=self.vertex_config)]

    def exclude_fields(self, k):
        return self.graph_config.exclude_fields(k)

    def set_current_resource_name(self, resource):
        self.current_fname = resource

    def apply(self, doc):
        return self.tree.apply(doc, self.vertex_config)
