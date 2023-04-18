import logging
import multiprocessing as mp
from collections import defaultdict
from functools import partial

from graph_cast.architecture import JConfigurator
from graph_cast.architecture.uitl import merge_documents, project_dicts
from graph_cast.util import timer as timer
from graph_cast.util.transform import pick_unique_dict

logger = logging.getLogger(__name__)


def jsondoc_to_collections(jsondoc, config: JConfigurator) -> defaultdict:
    """
    parse jsondoc using JConfigurator

    :param jsondoc: generic : {}
    :param config: JConfigurator
    :return: defaultdict vertex and edges collections
    """

    # acc = apply_mapper(config.json, jsondoc, config.vertex_config)

    acc = config.apply(jsondoc)

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


def jsonlike_to_collections(
    json_data: list,
    conf_obj: JConfigurator,
    ncores=1,
) -> tuple[defaultdict, defaultdict]:
    """

    :param json_data:
    :param conf_obj:
    :param ncores:
    :return:
    """

    with timer.Timer() as t_parse:
        kwargs = {"config": conf_obj}
        func = partial(jsondoc_to_collections, **kwargs)
        if ncores > 1:
            with mp.Pool(ncores) as p:
                default_dicts = p.map(func, json_data)
        else:
            default_dicts = list(map(func, json_data))

        super_dict = defaultdict(list)

        for d in default_dicts:
            for k, v in d.items():
                super_dict[k].extend(v)

    logger.info(
        " converting json to vertices and edges took"
        f" {t_parse.elapsed:.2f} sec"
    )

    vdocs = defaultdict(list)
    edocs = defaultdict(list)
    for k in super_dict.keys():
        if isinstance(k, tuple):
            edocs[k] = super_dict[k]
        else:
            vdocs[k] = super_dict[k]

    return vdocs, edocs
