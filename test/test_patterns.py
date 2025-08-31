import pathlib

from graflo.util.onto import Patterns


def test_patterns():
    pd = {"patterns": {"a": {"sub_path": "dir_a/dir_b"}, "b": {"regex": "^asd"}}}
    ps = Patterns.from_dict(pd)
    assert isinstance(ps.patterns["a"].sub_path / "a", pathlib.Path)
    assert str(ps.patterns["b"].sub_path / "a") == "a"
