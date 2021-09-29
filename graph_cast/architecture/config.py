class Configurator:
    # vertex_type -> vertex_collection_name
    vmap = {}

    # vertex_collection_name -> indices
    index_fields_dict = {}

    # vertex_collection_name -> extra_index
    extra_indices = {}

    # vertex_collection_name -> fields
    vfields = {}

    # list of blank collections
    blank_collections = []

    # vertex_collection_name -> [numeric fields]
    vcollection_numeric_fields_map = {}

    ### to change
    # table_type -> [ {vertex_collection :vc, map: (table field -> collection field)} ]

    # table_type -> (vertex_collection -> (table field -> collection field))
    ### TODO should be a dict of lists ?
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

    def __init__(self):
        pass

    def set_mode(self, mode):
        self.mode = mode

    # def vcol_map(self, vcol):
    #     return (
    #         self.table_collection_maps[self.mode][vcol]
    #         if vcol in self.table_collection_maps[self.mode]
    #         else dict()
    #     )

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
