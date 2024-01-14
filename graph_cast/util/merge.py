from __future__ import annotations

from collections import ChainMap


def discriminate(items, indices, discriminant_key, discriminant_value, fast=False):
    """

    :param items: list of documents (dict)
    :param indices:
    :param discriminant_key:
    :param discriminant_value:
    :param fast:
    :return: items
    """

    # pick items that have any of index field present
    _items = [item for item in items if any([k in item for k in indices])]

    if discriminant_value is not None:
        result = []
        for item in _items:
            if (
                discriminant_key in item
                and item[discriminant_key] == discriminant_value
            ):
                result += [item]
            if fast:
                break
        return result
    else:
        return _items


def merge_doc_basis(
    docs: list[dict],
    index_keys: tuple[str, ...],
    discriminant_key=None,
    discriminant_value=None,
) -> list[dict]:
    """

    :param docs:
    :param index_keys:
    :param discriminant_key:
    :param discriminant_value:
    :return:
    """

    # cast docs to sorted tuples keeping only indexes
    docs_tuplezied = [
        tuple(sorted((k, v) for k, v in item.items() if k in index_keys))
        for item in docs
    ]

    # pick bearing docs : those that differ by index_keys
    bearing_docs: dict[tuple, dict] = {q: dict() for q in set(docs_tuplezied)}

    # merge docs with respect to unique index key-value combinations
    for doc, doc_tuple in zip(docs, docs_tuplezied):
        bearing_docs[doc_tuple].update(doc)

    # merge docs without any index keys onto the first relevant doc
    if () in docs_tuplezied:
        relevant_docs = discriminate(
            docs, index_keys, discriminant_key, discriminant_value, fast=True
        )
        if relevant_docs:
            tuple_ix = tuple(
                sorted((k, v) for k, v in relevant_docs[0].items() if k in index_keys)
            )
            bearing_docs[tuple_ix].update(bearing_docs[()])
        del bearing_docs[()]

    return list(bearing_docs.values())


def merge_documents(docs: list[dict], main_key, discriminant_key, discriminant_value):
    """
    docs contain documents with main_key and documents without
    all docs without main_key should be merged with the doc that has doc[anchor_key] == anchor_value
    :param docs:
    :param main_key:
    :param discriminant_key:
    :param discriminant_value:
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
            if discriminant_key in item and item[discriminant_key] == discriminant_value
            else mains
        ).append(item)

    auxs += anchors
    r = [dict(ChainMap(*auxs))] + mains
    return r
