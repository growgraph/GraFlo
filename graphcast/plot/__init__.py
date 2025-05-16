"""Plotting utilities for graph visualization.

This module provides tools for visualizing graph schemas and structures.
It includes functionality for creating visual representations of graph
databases, their vertices, edges, and relationships.

Key Components:
    - SchemaPlotter: Creates visual representations of graph schemas

Example:
    >>> plotter = SchemaPlotter(schema)
    >>> plotter.plot("schema.png")
"""

from .plotter import SchemaPlotter

__all__ = ["SchemaPlotter"]
