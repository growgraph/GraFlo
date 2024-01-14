from graph_cast.util.merge import merge_doc_basis


def test_merge(merge_fixture):
    r = merge_doc_basis(
        merge_fixture,
        index_keys=("_key",),
        discriminant_key="__discriminant_key",
        discriminant_value="_top_level",
    )
    assert len(r) > 0
