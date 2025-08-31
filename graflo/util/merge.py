"""Document merging and discrimination utilities.

This module provides functions for merging and discriminating between documents
based on various criteria. It supports merging documents with common keys,
discriminating based on specific values, and handling different document structures.

Key Functions:
    - discriminate_by_key: Filter documents based on index fields and key presence
    - merge_doc_basis: Merge documents based on common index keys

"""

from graflo.architecture.onto import VertexRep


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


def merge_doc_basis_closest_preceding(
    docs: list[VertexRep],
    index_keys: tuple[str, ...],
) -> list[VertexRep]:
    """Merge VertexRep documents based on index_keys.

    Leading non-ID VertexReps are merged into the first ID VertexRep.
    Remaining non-ID VertexReps are merged into the closest preceding ID VertexRep.
    The merge is performed on the `vertex` attribute, and `ctx` dicts are merged among merged VertexReps.

    Args:
        docs: List of VertexRep to merge
        index_keys: Tuple of key names to use for merging

    Returns:
        list[VertexRep]: Merged VertexReps
    """
    merged_docs: list[VertexRep] = []
    pending_non_ids: list[VertexRep] = []

    def merge_vertex_ctx(target: VertexRep, sources: list[VertexRep]):
        # Merge vertex dicts
        for src in sources:
            target.vertex.update(src.vertex)
            target.ctx.update(src.ctx)
        return target

    for doc in docs:
        if any(k in doc.vertex for k in index_keys):
            # This is an ID VertexRep
            # First, handle any accumulated non-ID VertexReps
            if pending_non_ids:
                if not merged_docs:
                    # No previous ID doc, create new one with accumulated non-IDs
                    merged_doc = VertexRep(vertex={}, ctx={})
                    merged_doc = merge_vertex_ctx(merged_doc, pending_non_ids)
                    merged_docs.append(merged_doc)
                else:
                    # Merge accumulated non-IDs into the last ID doc
                    merged_docs[-1] = merge_vertex_ctx(merged_docs[-1], pending_non_ids)
                pending_non_ids.clear()

            # Add the current ID VertexRep (make a copy to avoid mutating input)
            merged_docs.append(VertexRep(vertex=doc.vertex.copy(), ctx=doc.ctx.copy()))
        else:
            # This is a non-ID VertexRep, accumulate it
            pending_non_ids.append(doc)

    # Handle any remaining non-ID VertexReps at the end
    if pending_non_ids and merged_docs:
        merged_docs[-1] = merge_vertex_ctx(merged_docs[-1], pending_non_ids)

    return merged_docs
