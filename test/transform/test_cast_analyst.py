import pytest

from graph_cast.util.transform import cast_ibes_analyst


@pytest.fixture()
def analyst_examples():
    return ["ADKINS/NARRA", "/ZHANG/LI/YA", "/ZHANG/LI", "ARFSTROM      J"]


@pytest.fixture()
def analyst_ref():
    return [
        ("ADKINS", "N"),
        ("ZHANG", "L"),
        ("ZHANG", "L"),
        ("ARFSTROM", "J"),
    ]


def test_cast_ibes_analyst(analyst_examples, analyst_ref):
    r = [cast_ibes_analyst(e) for e in analyst_examples]
    assert r == analyst_ref
