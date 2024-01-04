from __future__ import annotations

from collections import defaultdict

from graph_cast.architecture.onto import GraphContainer, GraphEntity


def project_dict(item, keys, how="include"):
    if how == "include":
        return {k: v for k, v in item.items() if k in keys}
    elif how == "exclude":
        return {k: v for k, v in item.items() if k not in keys}


def project_dicts(items, keys, how="include"):
    if how == "include":
        return [{k: v for k, v in item.items() if k in keys} for item in items]
    elif how == "exclude":
        return [
            {k: v for k, v in item.items() if k not in keys} for item in items
        ]
    else:
        raise ValueError(
            f" `how` should be exclude or include : instead {how}"
        )


def strip_prefix(dictlike, prefix="~"):
    new_dictlike = {}
    if isinstance(dictlike, dict):
        for k, v in dictlike.items():
            if isinstance(k, str):
                k = k.lstrip(prefix)
            new_dictlike[k] = strip_prefix(v, prefix)
    elif isinstance(dictlike, list):
        return [strip_prefix(x) for x in dictlike]
    else:
        return dictlike
    return new_dictlike


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
