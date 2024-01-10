from __future__ import annotations

import dataclasses
import pathlib

from graph_cast.onto import BaseDataclass


@dataclasses.dataclass
class FilePattern(BaseDataclass):
    regex: str | None = None
    sub_path: None | pathlib.Path = dataclasses.field(
        default_factory=lambda: pathlib.Path("./")
    )

    def __post_init__(self):
        if not isinstance(self.sub_path, pathlib.Path):
            self.sub_path = pathlib.Path(self.sub_path)
        assert self.sub_path is not None


@dataclasses.dataclass
class Patterns(BaseDataclass):
    patterns: dict[str, FilePattern] = dataclasses.field(default_factory=dict)
