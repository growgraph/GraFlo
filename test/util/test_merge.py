from graph_cast.util.merge import (
    discriminate,
    discriminate_by_key,
    merge_doc_basis,
)


def test_merge(merge_fixture, merge_result):
    r = merge_doc_basis(
        merge_fixture,
        index_keys=("_key",),
        discriminant_key="__discriminant_key",
    )
    r = sorted(r, key=lambda x: x["_key"])
    assert r == merge_result

    r = merge_doc_basis(
        merge_fixture,
        index_keys=("_key",),
        discriminant_key="__discriminant_key",
    )
    r = sorted(r, key=lambda x: x["_key"])
    assert r == merge_result


def test_discriminate(merge_fixture):
    r = discriminate(
        merge_fixture,
        indexes=("_key",),
        discriminant_key="__discriminant_key",
        discriminant_value="_top_level",
    )
    assert len(r) == 1

    r = discriminate_by_key(
        merge_fixture,
        indexes=("_key",),
        discriminant_key="__discriminant_key",
    )

    assert len(r) == 1

    r = discriminate_by_key(
        [
            {"_key": "a", "__discriminant_key": 0},
            {"_key": "b", "__discriminant_key": 0},
        ],
        indexes=("_key",),
        discriminant_key="__discriminant_key",
        fast=True,
    )
    assert len(r) == 1
