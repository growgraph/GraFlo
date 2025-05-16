"""Document merging and discrimination utilities.

This module provides functions for merging and discriminating between documents
based on various criteria. It supports merging documents with common keys,
discriminating based on specific values, and handling different document structures.

Key Functions:
    - discriminate_by_key: Filter documents based on index fields and key presence
    - merge_doc_basis: Merge documents based on common index keys

"""


def discriminate_by_key(items, indexes, discriminant_key, fast=False):
    """Filter documents based on index fields and key presence.

    This function filters a list of documents based on the presence of index fields
    and a specific key. It can operate in fast mode to return after finding the
    first match.

    Args:
        items: List of documents (dictionaries) to filter
        indexes: List of index field names to check for presence
        discriminant_key: Key to check for presence
        fast: Whether to return after first match (default: False)

    Returns:
        list[dict]: Filtered list of documents
    """
    # pick items that have any of index field present
    _items = [item for item in items if any(k in item for k in indexes)]

    if discriminant_key is not None:
        result = []
        for item in _items:
            if discriminant_key in item:
                result += [item]
                if fast:
                    break
        return result
    return _items


def merge_doc_basis(
    docs: list[dict],
    index_keys: tuple[str, ...],
    discriminant_key=None,
    # discriminant_value=None,
) -> list[dict]:
    """Merge documents based on common index keys.

    This function merges documents that share common index key-value combinations.
    Documents without index keys are merged with the first relevant document that
    has the discriminant key.

    Note:
        Currently works best with two groups of documents: those with and without
        the discriminant key. Future versions will support multiple discriminant
        value groups.

    Args:
        docs: List of documents to merge
        index_keys: Tuple of key names to use for merging
        discriminant_key: Optional key to use for merging documents without index keys

    Returns:
        list[dict]: Merged documents
    """
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
        relevant_docs = discriminate_by_key(
            docs, index_keys, discriminant_key, fast=True
        )
        if relevant_docs:
            tuple_ix = tuple(
                sorted((k, v) for k, v in relevant_docs[0].items() if k in index_keys)
            )
            bearing_docs[tuple_ix].update(bearing_docs.pop(()))

    return list(bearing_docs.values())
