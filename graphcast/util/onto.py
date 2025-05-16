"""Utility ontology classes for file patterns and configurations.

This module provides data classes for managing file patterns and configurations
used throughout the system. These classes support file discovery, pattern matching,
and configuration management.

Key Components:
    - FilePattern: Configuration for file pattern matching
    - Patterns: Collection of named file patterns
"""

import dataclasses
import pathlib

from graphcast.onto import BaseDataclass


@dataclasses.dataclass
class FilePattern(BaseDataclass):
    """Configuration for file pattern matching.

    This class defines a pattern for matching files, including a regular expression
    for matching filenames and a subdirectory path to search in.

    Args:
        regex: Regular expression pattern for matching filenames
        sub_path: Path to search for matching files (default: "./")

    Attributes:
        regex: Regular expression pattern
        sub_path: Path to search in
    """

    regex: str | None = None
    sub_path: None | pathlib.Path = dataclasses.field(
        default_factory=lambda: pathlib.Path("./")
    )

    def __post_init__(self):
        """Initialize and validate the file pattern.

        Ensures that sub_path is a Path object and is not None.
        """
        if not isinstance(self.sub_path, pathlib.Path):
            self.sub_path = pathlib.Path(self.sub_path)
        assert self.sub_path is not None


@dataclasses.dataclass
class Patterns(BaseDataclass):
    """Collection of named file patterns.

    This class manages a collection of file patterns, each associated with a name.
    It provides a way to organize and access multiple file patterns.

    Args:
        patterns: Dictionary mapping names to FilePattern instances

    Attributes:
        patterns: Dictionary of named file patterns
    """

    patterns: dict[str, FilePattern] = dataclasses.field(default_factory=dict)
