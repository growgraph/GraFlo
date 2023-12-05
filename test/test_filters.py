import pytest

from graph_cast.onto import (
    Clause,
    ComparisonOperator,
    LeafClause,
    LogicalOperator,
    init_filter,
)


@pytest.fixture()
def eq_clause():
    # doc.x == 1
    return ["==", "1", "x"]


@pytest.fixture()
def cong_clause():
    # doc.x % 3 == 2
    return ["==", 2, "y", "% 2"]


@pytest.fixture()
def in_clause():
    return ["IN", [1, 2]]


@pytest.fixture()
def and_clause(eq_clause, cong_clause):
    return {"AND": [eq_clause, cong_clause]}


def test_leaf_clause_construct(eq_clause):
    lc = LeafClause(*eq_clause)
    assert lc.cmp_operator == ComparisonOperator.EQ
    assert lc.cast_filter() == 'doc["x"] == "1"'


def test_init_filter_and(and_clause):
    c = init_filter(and_clause)
    assert c.operator == LogicalOperator.AND
    assert c.cast_filter() == 'doc["x"] == "1" AND doc["y"] % 2 == 2'


def test_init_filter_eq(eq_clause):
    c = init_filter(eq_clause)
    assert c.cast_filter() == 'doc["x"] == "1"'


def test_init_filter_in(in_clause):
    c = init_filter(in_clause)
    assert c.cast_filter() == "IN [1, 2]"
