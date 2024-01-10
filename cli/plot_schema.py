import logging
import sys

import click

from graph_cast.plot.plotter import SchemaPlotter

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
    """
    split a set of weights into bag (groups) of total weight of at most threshold weight
    :param weights:
    :param ks_size:
    :return:
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
@click.option("-c", "--schema-path", type=click.Path())
@click.option("-o", "--figure-output-path", type=click.Path())
@click.option("-p", "--prune-low-degree-nodes", type=bool, default=True)
def plot_schema(schema_path, figure_output_path, prune_low_degree_nodes):
    """
    plot graph_cast schema
    """
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    plotter = SchemaPlotter(schema_path, figure_output_path)
    plotter.plot_vc2fields()
    plotter.plot_source2vc()
    plotter.plot_vc2vc(prune_leaves=prune_low_degree_nodes)
    plotter.plot_source2vc_detailed()


if __name__ == "__main__":
    plot_schema()
