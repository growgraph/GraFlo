import logging
from collections import defaultdict

from graph_cast.architecture import Configurator
from graph_cast.architecture.onto import GraphEntity, TypeVE
from graph_cast.architecture.resource import Resource
from graph_cast.architecture.util import project_dicts
from graph_cast.util.merge import merge_documents
from graph_cast.util.transform import pick_unique_dict

logger = logging.getLogger(__name__)


def list_to_dict_vertex(
    list_default_dicts: list[defaultdict[TypeVE, list]],
) -> defaultdict[str, list]:
    """

    :param list_default_dicts:
    :return:
    """
    super_dict = defaultdict(list)

    for d in list_default_dicts:
        for k, v in d.items():
            # choose either vertices or edges, depending on vertices_not_edges
            if isinstance(k, str):
                super_dict[k].extend(v)
    return super_dict


def list_to_dict_edges(
    list_default_dicts: list[defaultdict[TypeVE, list]],
) -> defaultdict[tuple[str, str], list]:
    super_dict = defaultdict(list)

    for d in list_default_dicts:
        for k, v in d.items():
            # choose either vertices or edges, depending on vertices_not_edges
            if isinstance(k, tuple):
                super_dict[k].extend(v)
    return super_dict


def normalize_unit(
    unit_doc: defaultdict[TypeVE, list], config: Configurator
) -> defaultdict[TypeVE, list]:
    """

    :param unit_doc: generic : {}
    :param config: JConfigurator
    :return: defaultdict vertex and edges collections
    """

    for vertex, v in unit_doc.items():
        v = pick_unique_dict(v)
        # (vertex is a vertex col) ~ not isinstance(k, tuple)
        if not isinstance(vertex, tuple):
            if config.exclude_fields(vertex):
                v = project_dicts(
                    v, config.exclude_fields(vertex), how="exclude"
                )
        if vertex in config.merge_collections:
            v = merge_documents(v)
        unit_doc[vertex] = v
    return unit_doc
