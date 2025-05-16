from graphcast.util.merge import (
    merge_doc_basis,
)


def test_merge_simple(docs_simple):
    r = merge_doc_basis(docs_simple, ("id",))
    assert len(r) == 1
    assert r[0]["a"] == 2
    assert r[0]["b"] == 1


def test_merge_simple_two_doc(docs_simple_two_doc):
    r = merge_doc_basis(docs_simple_two_doc, ("id",))
    assert len(r) == 2


def test_merge(merge_input_with_discriminant, merge_output):
    r = merge_doc_basis(
        merge_input_with_discriminant,
        index_keys=("_key",),
        discriminant_key="__discriminant_key",
    )
    r = sorted(r, key=lambda x: x["_key"])
    assert r == merge_output

    r = merge_doc_basis(
        merge_input_with_discriminant,
        index_keys=("_key",),
        discriminant_key="__discriminant_key",
    )
    r = sorted(r, key=lambda x: x["_key"])
    assert r == merge_output


def test_merge_nodiscriminant(merge_input_no_disc, merge_output_no_disc):
    r = merge_doc_basis(
        merge_input_no_disc,
        index_keys=("_key",),
        # discriminant_key="__discriminant_key",
    )
    r = sorted(r, key=lambda x: x["_key"])
    assert r == merge_output_no_disc
