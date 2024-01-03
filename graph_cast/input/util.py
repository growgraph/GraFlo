import logging
from collections import defaultdict

from graph_cast.architecture.onto import GraphContainer, GraphEntity, TypeVE
from graph_cast.architecture.util import project_dicts
from graph_cast.util.merge import merge_documents
from graph_cast.util.transform import pick_unique_dict

logger = logging.getLogger(__name__)


def list_docs_to_graph_container(
    list_default_dicts: list[defaultdict[GraphEntity, list]]
) -> GraphContainer:
    vdict: defaultdict[str, list] = defaultdict(list)
    edict: defaultdict[tuple[str, str, str | None], list] = defaultdict(list)

    for d in list_default_dicts:
        for k, v in d.items():
            if isinstance(k, str):
                vdict[k].extend(v)
            elif isinstance(k, tuple):
                assert (
                    len(k) == 3
                    and all(isinstance(item, str) for item in k[:-1])
                    and isinstance(k[-1], (str, type(None)))
                )
                edict[k].extend(v)
    return GraphContainer(
        vertices=dict(vdict.items()),
        edges=dict(edict.items()),
        linear=list_default_dicts,
    )
