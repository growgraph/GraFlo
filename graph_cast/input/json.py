from typing import DefaultDict
from graph_cast.architecture import JConfigurator
from graph_cast.input.json_aux import apply_mapper, project_dicts, merge_documents
from graph_cast.util.transform import pick_unique_dict


def jsonlike_to_collections(jsondoc, config: JConfigurator) -> DefaultDict:
    """
    top level function such that (json, config) -> docs]
    :param jsondoc:
    :param config:
    :return:
    """

    acc = apply_mapper(config.json, jsondoc, config.vertex_config)

    for k, v in acc.items():
        v = pick_unique_dict(v)
        # (k is a vertex col) ~ not isinstance(k, tuple)
        if not isinstance(k, tuple):
            if config.exclude_fields(k):
                v = project_dicts(v, config.exclude_fields(k), how="exclude")
        if k in config.merge_collections:
            v = merge_documents(v)
        acc[k] = v
    return acc
