class Configurator:
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
        self.table_config = TableConfig(config["csv"])

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
        if self.mode in self.table_config.tables:
            return self.table_config.table_collection_maps[self.mode]
        else:
            return None


class TableConfig:
    # table_type -> [ {vertex_collection :vc, map: (table field -> collection field)} ]
    # conf_obj.table_collection_maps = parse_input_output_field_map(config["csv"])
    #
    # conf_obj.transformation_maps = parse_transformations(config["csv"])
    #
    # conf_obj.encodings = parse_encodings(config["csv"])
    #
    # conf_obj.logic = parse_logic(config["csv"])
    #
    # parse_modes2graphs(config["csv"], conf_obj)
    #
    # parse_weights(config["csv"], conf_obj)

    table_collection_maps = dict()

    _tables = set()

    def __init__(self, vconfig):
        self._init_tables(vconfig)
        self._init_transformations(vconfig)

    def _init_tables(self, vconfig):
        self._tables = set([item["tabletype"] for item in vconfig if "tabletype" in item])

    # TODO verify against VertexConfig
    # work out encapsulation
    def _init_transformations(self, subconfig):
        for item in subconfig:
            self.table_collection_maps[item["tabletype"]] = [
                {"type": vc["type"], "map": vc["map_fields"]}
                for vc in item["vertex_collections"]
                if "map_fields" in vc
            ]

    @property
    def tables(self):
        return self._tables


class VertexConfig:
    _vcollections = set()

    # vertex_type -> vertex_collection_name
    _vmap = {}

    # vertex_collection_name -> indices
    _index_fields_dict = {}

    # vertex_collection_name -> extra_index
    _extra_indices = {}

    # vertex_collection_name -> fields
    _vfields = {}

    # vertex_collection_name -> [numeric fields]
    _vcollection_numeric_fields_map = {}

    # list of blank collections
    _blank_collections = set()

    def __init__(self, vconfig):
        config = vconfig["collections"]
        self._init_vcollections(config)
        self._init_names(config)
        self._init_indices(config)
        self._init_extra_indexes(config)

        self._init_fields(config)
        self._init_numeric_fields(config)
        if "blanks" in vconfig:
            self._init_blank_collections(vconfig["blanks"])

    @property
    def collections(self):
        return self._vcollections

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
        self._blank_collections = set(vconfig)
        if set(self._blank_collections) - set(self._vcollections):
            raise ValueError(
                f" Blank collections {self.blank_collections} are not defined as vertex collections"
            )

    @property
    def blank_collections(self):
        return iter(self._blank_collections)

    def _init_fields(self, vconfig):
        self._vfields = {k: v["fields"] for k, v in vconfig.items() if "fields" in v}

    def fields(self, vertex_name):
        if vertex_name in self._vcollections:
            if vertex_name in self._vfields:
                return self._vfields[vertex_name]
            else:
                return ()
        else:
            raise ValueError(
                f" Accessing vertex collection fields: vertex collection {vertex_name} was not defined in config"
            )

    def _init_numeric_fields(self, vconfig):
        self._vcollection_numeric_fields_map = {
            k: v["numeric_fields"] for k, v in vconfig.items() if "numeric_fields" in v
        }

    def numeric_fields_list(self, vertex_name):
        if vertex_name in self._vcollections:
            if vertex_name in self._vcollection_numeric_fields_map:
                return self._vcollection_numeric_fields_map[vertex_name]
            else:
                return ()
        else:
            raise ValueError(
                f" Accessing vertex collection numeric fields: vertex collection {vertex_name} was not defined in config"
            )
