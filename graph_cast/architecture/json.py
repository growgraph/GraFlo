from copy import deepcopy

from graph_cast.architecture.general import Configurator
from graph_cast.architecture.ptree import ParsingTree


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
        if "extra" in config:
            cconfig = config["extra"]
            if "merge_collections" in cconfig:
                self.merge_collections = tuple(cconfig["merge_collections"])

    def exclude_fields(self, k):
        return self.graph_config.exclude_fields(k)

    def set_current_resource_name(self, resource):
        self.current_fname = resource

    def apply(self, doc):
        return self.tree.apply(doc, self.vertex_config)
