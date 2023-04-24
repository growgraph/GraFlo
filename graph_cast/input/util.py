import logging
from collections import defaultdict

from graph_cast.architecture.schema import TypeVE

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
