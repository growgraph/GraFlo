import logging

logger = logging.getLogger(__name__)


def parse_vcollection(config):

    # vertex_type -> vertex_collection_name
    vmap = {
        k: f'{v["basename"]}' for k, v in config["vertex_collections"].items()
    }

    # vertex_collection_name -> field_definition
    index_fields_dict = {k: v["index"] for k, v in config["vertex_collections"].items()}

    # vertex_collection_name -> extra_index
    # in addition to index from field_definition
    extra_indices = {
        k: v["extra_index"]
        for k, v in config["vertex_collections"].items()
        if "extra_index" in v
    }

    # vertex_collection_name -> fields_keep
    retrieve_fields_dict = {
        k: v["fields"] for k, v in config["vertex_collections"].items()
    }
    return vmap, index_fields_dict, extra_indices

