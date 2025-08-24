import pytest

from graphcast.architecture.onto import (
    LocationIndex,
)


def test_lindex():
    la = LocationIndex(("a", "b", "c"))
    lb = LocationIndex(("a", "b", "c"))
    lc = LocationIndex(("a", "b", "d"))
    ld = LocationIndex((0,))
    assert la.equality_index(lb) == 3
    assert la.equality_index(lc) == 2
    assert la.equality_index(ld) == 0
