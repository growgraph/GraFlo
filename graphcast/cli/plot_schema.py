"""Schema visualization tool for graph databases.

This module provides functionality for visualizing graph database schemas using Graphviz.
It includes tools for plotting vertex-to-vertex relationships, vertex fields, and resource
mappings. The module supports various visualization options and graph layout customization.

Key Components:
    - SchemaPlotter: Main class for schema visualization
    - knapsack: Utility for optimizing graph layout
    - plot_schema: CLI command for schema visualization

Graphviz Attributes Reference:
    - https://renenyffenegger.ch/notes/tools/Graphviz/attributes/index
    - https://rsms.me/graphviz/
    - https://graphviz.readthedocs.io/en/stable/examples.html
    - https://graphviz.org/doc/info/attrs.html

Example:
    >>> plot_schema(schema_path="schema.yaml", figure_output_path="schema.png")
"""

import logging
import sys

import click

from graphcast.plot.plotter import SchemaPlotter

"""

graphviz attributes 

https://renenyffenegger.ch/notes/tools/Graphviz/attributes/index
https://rsms.me/graphviz/
https://graphviz.readthedocs.io/en/stable/examples.html
https://graphviz.org/doc/info/attrs.html

usage: 
    color='red',style='filled', fillcolor='blue',shape='square'

to keep 
level_one = [node1, node2]
sg_one = ag.add_subgraph(level_one, rank='same')

"""


def knapsack(weights, ks_size=7):
    """Split a set of weights into groups of at most threshold weight.

    This function implements a greedy algorithm to partition weights into groups
    where each group's total weight is at most ks_size. It's used for optimizing
    graph layout by balancing node distribution.

    Args:
        weights: List of weights to partition
        ks_size: Maximum total weight per group (default: 7)

    Returns:
        list[list[int]]: List of groups, where each group is a list of indices
            from the original weights list

    Raises:
        ValueError: If any single weight exceeds ks_size

    Example:
        >>> weights = [3, 4, 2, 5, 1]
        >>> knapsack(weights, ks_size=7)
        [[4, 0, 2], [1, 3]]  # Groups with weights [6, 7]
    """
    pp = sorted(list(zip(range(len(weights)), weights)), key=lambda x: x[1])
    print(pp)
    acc = []
    if pp[-1][1] > ks_size:
        raise ValueError("One of the items is larger than the knapsack")

    while pp:
        w_item = []
        w_item += [pp.pop()]
        ww_item = sum([item for _, item in w_item])
        while ww_item < ks_size:
            cnt = 0
            for j, item in enumerate(pp[::-1]):
                diff = ks_size - item[1] - ww_item
                if diff >= 0:
                    cnt += 1
                    w_item += [pp.pop(len(pp) - j - 1)]
                    ww_item += w_item[-1][1]
                else:
                    break
            if ww_item >= ks_size or cnt == 0:
                acc += [w_item]
                break
    acc_ret = [[y for y, _ in subitem] for subitem in acc]
    return acc_ret


@click.command()
@click.option("-c", "--schema-path", type=click.Path(), required=True)
@click.option("-o", "--figure-output-path", type=click.Path(), required=True)
@click.option("-p", "--prune-low-degree-nodes", type=bool, default=False)
def plot_schema(schema_path, figure_output_path, prune_low_degree_nodes):
    """Generate visualizations of the graph database schema.

    This command creates multiple visualizations of the schema:
    1. Vertex-to-vertex relationships
    2. Vertex fields and their relationships
    3. Resource mappings

    The visualizations are saved to the specified output path.

    Args:
        schema_path: Path to the schema configuration file
        figure_output_path: Path where the visualization will be saved
        prune_low_degree_nodes: Whether to remove nodes with low connectivity
            from the visualization (default: False)

    Example:
        $ uv run plot_schema -c schema.yaml -o schema.png
    """
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    plotter = SchemaPlotter(schema_path, figure_output_path)
    plotter.plot_vc2vc(prune_leaves=prune_low_degree_nodes)
    plotter.plot_vc2fields()
    plotter.plot_resources()
    # plotter.plot_source2vc()
    # plotter.plot_source2vc_detailed()


if __name__ == "__main__":
    plot_schema()
