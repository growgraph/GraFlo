from collections import defaultdict
from copy import deepcopy
from graph_cast.architecture.general import Configurator


class JConfigurator(Configurator):
    eedges = []
    mfields = defaultdict(list)

    def __init__(self, config):
        super().__init__(config)
        self.json = deepcopy(config["json"])

    def graph(self, u, v):
        return self.graph_config.graph(u, v)

    def exclude_fields(self, k):
        return self.graph_config.exclude_fields(k)
