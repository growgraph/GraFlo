from collections import ChainMap

from graph_cast.architecture.onto import ANCHOR_KEY


def merge_doc_basis(
    docs: list[dict], index_keys: tuple[str, ...]
) -> list[dict]:
    """

    :param docs:
    :param index_keys:
    :return:
    """

    # cast each doc to a sorted tuple keeping only keys from keys
    docs_tuplezied = [
        tuple(
            sorted({k: v for k, v in item.items() if k in index_keys}.items())
        )
        for item in docs
    ]

    docs_tuplezied_non_null = [x for x in docs_tuplezied if x]

    # pick bearing docs : those that differ by index_keys
    bearing_docs: dict[tuple, dict] = {q: dict() for q in set(docs_tuplezied)}

    # merge docs with respect to unique index key-value combinations
    for doc, doc_tuple in zip(docs, docs_tuplezied):
        bearing_docs[doc_tuple].update(doc)

    # merge docs without any index keys onto the first relevant doc
    if docs_tuplezied_non_null:
        tuple_ix = docs_tuplezied_non_null[0]
        if () in docs_tuplezied:
            bearing_docs[tuple_ix].update(bearing_docs[()])
            del bearing_docs[()]

    return list(bearing_docs.values())


def merge_documents(
    docs: list[dict],
    main_key="_key",
    anchor_key=ANCHOR_KEY,
    anchor_value="main",
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
    mains_: list = []
    mains: list = []
    auxs: list = []
    anchors: list = []
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
