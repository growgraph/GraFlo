from collections import defaultdict
from copy import deepcopy
from graph_cast.architecture.general import Configurator


class JConfigurator(Configurator):
    def __init__(self, config):
        super().__init__(config)

        self.eedges = []
        self.mfields = defaultdict(list)

        self.json = deepcopy(config["json"])
        # which collections should be merged? (if they are found in different parts of json doc)
        self.merge_collections = tuple()
        if "extra" in config:
            cconfig = config["extra"]
            if "merge_collections" in cconfig:
                self.merge_collections = tuple(cconfig["merge_collections"])

    def graph(self, u, v):
        return self.graph_config.graph(u, v)

    def exclude_fields(self, k):
        return self.graph_config.exclude_fields(k)
