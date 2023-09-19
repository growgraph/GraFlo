import logging

import pytest
import yaml

from graph_cast.architecture.filter import Filter, Operator, UnitOperand

logger = logging.getLogger(__name__)


@pytest.fixture()
def unit_operand_open():
    s = yaml.safe_load("""
        field: name
        foo: __eq__
        value: Open
    """)
    return s


@pytest.fixture()
def unit_operand_close():
    s = yaml.safe_load("""
        field: name
        foo: __eq__
        value: Close
    """)
    return s


@pytest.fixture()
def unit_operand_volume():
    s = yaml.safe_load("""
        field: name
        foo: __ne__
        value: Volume
    """)
    return s


@pytest.fixture()
def unit_operand_b():
    s = yaml.safe_load("""
        field: value
        foo: __gt__
        value: 0
    """)
    return s


@pytest.fixture()
def filter_a(unit_operand_open, unit_operand_b):
    s = {"and": [unit_operand_open, unit_operand_b]}
    return s


@pytest.fixture()
def filter_ab(unit_operand_open, unit_operand_close, unit_operand_b):
    s = {
        Operator.OR: [
            {Operator.AND: [unit_operand_open, unit_operand_b]},
            {Operator.AND: [unit_operand_close, unit_operand_b]},
        ]
    }
    return s


@pytest.fixture()
def filter_implication(unit_operand_open, unit_operand_b):
    s = {"if_then": [unit_operand_open, unit_operand_b]}
    return s


def test_condition(unit_operand_open):
    m = UnitOperand(**unit_operand_open)
    doc = {"name": "Open"}
    assert m(**doc)


def test_condition_b(unit_operand_b):
    m = UnitOperand(**unit_operand_b)
    doc = {"value": -1}
    assert not m(**doc)


def test_filter_a(filter_a):
    m = Filter(filter_a)

    doc = {"name": "Open", "value": 5.0}
    assert m(doc)

    doc = {"name": "Open", "value": -1.0}
    assert not m(doc)


def test_filter_ab(filter_ab):
    m = Filter(filter_ab)

    doc = {"name": "Open", "value": 5.0}
    assert m(doc)

    doc = {"name": "Open", "value": -1.0}
    assert not m(doc)

    doc = {"name": "Close", "value": 5.0}
    assert m(doc)

    doc = {"name": "Close", "value": -1.0}
    assert not m(doc)


def test_filter_implication(filter_implication):
    m = Filter(filter_implication)

    doc = {"name": "Open", "value": -1.0}
    assert not m(doc)

    doc = {"name": "Close", "value": -1.0}
    assert m(doc)


def test_filter_neq(unit_operand_volume):
    m = Filter(unit_operand_volume)

    doc = {"name": "Open", "value": -1.0}
    assert m(doc)

    doc = {"name": "Volume", "value": -1.0}
    assert not m(doc)
