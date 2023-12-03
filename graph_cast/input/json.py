import logging
import multiprocessing as mp
from collections import defaultdict
from functools import partial

from graph_cast.architecture import JConfigurator
from graph_cast.architecture.schema import TypeVE
from graph_cast.input.util import normalize_unit
from graph_cast.util import timer as timer

logger = logging.getLogger(__name__)


def jsondoc_to_collections(
    jsondoc, config: JConfigurator
) -> defaultdict[TypeVE, list]:
    """
    parse jsondoc using JConfigurator

    :param jsondoc: generic : {}
    :param config: JConfigurator
    :return: defaultdict vertex and edges collections
    """

    acc = config.apply(jsondoc)
    acc = normalize_unit(acc, config)
    return acc


def jsonlike_to_collections(
    json_data: list,
    conf_obj: JConfigurator,
    ncores=1,
) -> list[defaultdict[TypeVE, list]]:
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
                list_defaultdicts = p.map(func, json_data)
        else:
            list_defaultdicts = list(map(func, json_data))

    logger.info(
        " converting json to vertices and edges took"
        f" {t_parse.elapsed:.2f} sec"
    )

    return list_defaultdicts
