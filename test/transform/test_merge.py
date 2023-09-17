import pytest

from graph_cast.util.merge import merge_doc_basis, merge_documents


@pytest.fixture
def docs_simple():
    return [{"id": 0, "a": 2}, {"id": 0, "b": 1}]


@pytest.fixture
def docs_simple_two_doc():
    return [{"id": 0, "a": 2}, {"id": 1, "b": 1}]


@pytest.fixture
def docs_complex_two_doc():
    return [{"id": 0, "a": 2}, {"b": 1}]


def test_merge_simple(docs_simple):
    r = merge_doc_basis(docs_simple, ("id",))
    assert len(r) == 1
    assert r[0]["a"] == 2
    assert r[0]["b"] == 1


def test_merge_simple_two_doc(docs_simple_two_doc):
    r = merge_doc_basis(docs_simple_two_doc, ("id",))
    assert len(r) == 2
