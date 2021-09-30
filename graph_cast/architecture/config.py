class Configurator:
    # table_type -> [ (vertex_collection , (table field -> collection field))]
    table_collection_maps = {}

    # table_type -> transforms
    transformation_maps = {}

    # table_type -> encodings
    encodings = {}

    # table_type -> extra logic
    logic = {}

    graphs_def = {}

    # table_type -> [{collection: cname, collection_maps: maps}]
    modes2collections = {}
    modes2graphs = {}

    weights_definition = {}

    mode = None

    def __init__(self, config):
        self.vertex_config = VertexConfig(config["vertex_collections"])

    def set_mode(self, mode):
        self.mode = mode

    @property
    def encoding(self):
        if self.mode in self.encodings:
            return self.encodings[self.mode]
        else:
            return None

    @property
    def current_graphs(self):
        if self.mode in self.modes2graphs:
            return self.modes2graphs[self.mode]
        else:
            return []

    @property
    def current_collections(self):
        if self.mode in self.modes2collections:
            return self.modes2collections[self.mode]
        else:
            return None

    @property
    def current_transformations(self):
        if self.mode in self.transformation_maps:
            return self.transformation_maps[self.mode]
        else:
            return None

    @property
    def current_weights(self):
        if self.mode in self.weights_definition:
            return self.weights_definition[self.mode]
        else:
            return None

    @property
    def current_field_maps(self):
        if self.mode in self.table_collection_maps:
            return self.table_collection_maps[self.mode]
        else:
            return None


class VertexConfig:
    _vcollections = set()

    # vertex_type -> vertex_collection_name
    _vmap = {}

    # vertex_collection_name -> indices
    _index_fields_dict = {}

    # vertex_collection_name -> extra_index
    _extra_indices = {}

    # vertex_collection_name -> fields
    vfields = {}

    # list of blank collections
    blank_collections = []

    # vertex_collection_name -> [numeric fields]
    vcollection_numeric_fields_map = {}

    def __init__(self, vconfig):
        config = vconfig["collections"]
        self._init_vcollections(config)
        self._init_names(config)
        self._init_indices(config)
        self._init_extra_indexes(config)

        self._init_fields(config)
        self._init_numeric_fields(config)
        self._init_blank_collections(vconfig["blanks"])

    @property
    def collections(self):
        return iter(self._vcollections)

    def _init_vcollections(self, vconfig):
        self._vcollections = set(vconfig.keys())

    def _init_names(self, vconfig):
        try:
            self._vmap = {
                k: v["basename"] for k, v in vconfig.items() if "basename" in v
            }
        except:
            raise KeyError(
                "vconfig does not have 'basename' for one of the vertex collections!"
            )

    def name(self, vertex_name):
        # old vmap
        if vertex_name in self._vcollections:
            if vertex_name in self._vmap:
                return self._vmap[vertex_name]
            else:
                return vertex_name
        else:
            raise ValueError(
                f" Accessing vertex collection names: vertex collection {vertex_name} was not defined in config"
            )

    def _init_indices(self, vconfig):
        self._index_fields_dict = {
            k: v["index"] for k, v in vconfig.items() if "index" in v
        }

    def index(self, vertex_name):
        # old index_fields_dict
        if vertex_name in self._vcollections:
            if vertex_name in self._index_fields_dict:
                return self._index_fields_dict[vertex_name]
            else:
                return ["_key"]
        else:
            raise ValueError(
                f" Accessing vertex collection indexes: vertex collection {vertex_name} was not defined in config"
            )

    def _init_extra_indexes(self, vconfig):
        self._extra_indices = {
            k: v["extra_index"] for k, v in vconfig.items() if "extra_index" in v
        }

    def extra_index_list(self, vertex_name):
        # old index_fields_dict
        if vertex_name in self._vcollections:
            if vertex_name in self._extra_indices:
                return self._extra_indices[vertex_name]
            else:
                return ()
        else:
            raise ValueError(
                f" Accessing vertex collection indexes: vertex collection {vertex_name} was not defined in config"
            )

    def _init_blank_collections(self, vconfig):
        self.blank_collections = vconfig
        if set(self.blank_collections) - set(self._vcollections):
            raise ValueError(f" Blank collections {self.blank_collections} are not defined as vertex collections")

    def _init_fields(self, vconfig):
        self.vfields = {
            k: (v["fields"] if "fields" in v else []) for k, v in vconfig.items()
        }

    def _init_numeric_fields(self, vconfig):
        self.vcollection_numeric_fields_map = {
            k: v["numeric_fields"] for k, v in vconfig.items() if "numeric_fields" in v
        }
