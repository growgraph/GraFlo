from collections import ChainMap

from graph_cast.architecture.schema import _anchor_key


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


def merge_documents(
    docs, main_key="_key", anchor_key=_anchor_key, anchor_value="main"
):
    """
    docs contain documents with main_key and documents without
    all docs without main_key should be merged with the doc that has doc[anchor_key] == anchor_value
    :param docs:
    :param main_key:
    :param anchor_key:
    :param anchor_value:
    :return: list of docs, each of which contains main_key
    """
    mains_, mains, auxs, anchors = [], [], [], []
    # split docs into two groups with and without main_key
    for item in docs:
        (mains_ if main_key in item else auxs).append(item)

    for item in mains_:
        (
            anchors
            if anchor_key in item and item[anchor_key] == anchor_value
            else mains
        ).append(item)

    auxs += anchors
    r = [dict(ChainMap(*auxs))] + mains
    return r
